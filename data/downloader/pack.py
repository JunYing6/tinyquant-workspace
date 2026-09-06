"""Kline packing: per-day bar files -> the yearly ``kline`` duckdb table.

The daily bar scopes (``trade_data/daily``, ``index/daily``, ``fund/daily``)
land each trading day's vendor rows at ``{year}/{MMDD}/kline.parquet`` — the
raw layer.  This module packs those files into ``{year}/year.duckdb`` table
``kline`` (the layout the ``market.bar`` adapters read) and derives the
multi-day timeframes (5d/10d) from the 1d rows:

- per instrument (``data_type`` + ``ts_code``), the year's 1d rows sorted by
  ``trade_date`` are chunked into consecutive blocks of N trading days;
- each block aggregates to one row: open = first open, high/low = extremes,
  close = last close, vol/amount = sums, ``trade_date`` = block's last day,
  ``adj_factor``/``name`` = block's last row, ``pre_close`` = previous block's
  close (NaN for the first block), ``pct_chg``/``change`` recomputed;
- ``timeframe`` is ``"5d"``/``"10d"``.

Packing is idempotent: 1d rows are merged from the daily files (business key
``ts_code``+``trade_date``+``data_type``+``timeframe``, keep last), derived
timeframes are recomputed from scratch, and the table is replaced atomically.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd

from data.downloader.base import duckdb_table_exists

logger = logging.getLogger(__name__)

KLINE_TABLE = "kline"
KLINE_BASE_TIMEFRAME = "1d"
KLINE_DEDUPE_KEYS = ["ts_code", "trade_date", "data_type", "timeframe"]

DERIVED_TIMEFRAMES: tuple[tuple[str, int], ...] = (("5d", 5), ("10d", 10))


def pack_kline(
    root: str | Path,
    year: str | int,
    *,
    derived_timeframes: tuple[tuple[str, int], ...] = DERIVED_TIMEFRAMES,
) -> Path:
    """Pack ``{root}/{year}/{MMDD}/kline.parquet`` into ``{root}/{year}/year.duckdb``.

    Returns the duckdb path.  Safe to re-run any time; the 1d rows are merged
    from the daily files and the multi-day rows are recomputed.
    """
    year_dir = Path(root) / str(year)
    db_path = year_dir / "year.duckdb"

    day_files = sorted(year_dir.glob("*/kline.parquet"))
    file_rows = (
        pd.concat((pd.read_parquet(p) for p in day_files), ignore_index=True)
        if day_files
        else pd.DataFrame()
    )
    if not file_rows.empty and "timeframe" in file_rows.columns:
        file_rows = file_rows[file_rows["timeframe"] == KLINE_BASE_TIMEFRAME]

    base_1d = _merge_base_1d(db_path, file_rows)
    if base_1d.empty:
        logger.warning("[pack] %s 无 1d 行，跳过 kline 打包", year_dir)
        return db_path

    frames = [base_1d]
    for label, size in derived_timeframes:
        derived = derive_multi_day_bars(base_1d, size)
        if not derived.empty:
            frames.append(derived)
            logger.info("[pack] %s 派生 %s: %d 行", year, label, len(derived))
    table = pd.concat(frames, ignore_index=True)
    table = table.drop_duplicates(subset=KLINE_DEDUPE_KEYS, keep="last")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.register("_kline_df", table)
        con.execute(f'CREATE OR REPLACE TABLE "{KLINE_TABLE}" AS SELECT * FROM _kline_df')
        con.unregister("_kline_df")
    finally:
        con.close()
    logger.info("[pack] %s kline 表 %d 行 (含派生)", db_path, len(table))
    return db_path


def _merge_base_1d(db_path: Path, file_rows: pd.DataFrame) -> pd.DataFrame:
    """Existing table's 1d rows merged with the daily-file rows (keep last)."""
    if not db_path.is_file():
        return file_rows
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not duckdb_table_exists(con, KLINE_TABLE):
            return file_rows
        existing = con.execute(
            f'SELECT * FROM "{KLINE_TABLE}" WHERE timeframe = ?', [KLINE_BASE_TIMEFRAME]
        ).df()
    finally:
        con.close()
    if existing.empty:
        return file_rows
    if file_rows.empty:
        return existing
    combined = pd.concat([existing, file_rows], ignore_index=True)
    return combined.drop_duplicates(subset=KLINE_DEDUPE_KEYS, keep="last")


def derive_multi_day_bars(base_1d: pd.DataFrame, size: int) -> pd.DataFrame:
    """Aggregate 1d rows into consecutive N-trading-day blocks per instrument."""
    if base_1d.empty:
        return pd.DataFrame()
    required = {"ts_code", "trade_date", "data_type", "open", "high", "low", "close"}
    missing = required - set(base_1d.columns)
    if missing:
        raise ValueError(f"kline 1d rows missing columns for derivation: {sorted(missing)}")

    frame = base_1d.copy()
    frame["trade_date"] = frame["trade_date"].astype("string")
    frame = frame[frame["trade_date"].notna()]  # undated rows cannot form bars
    frame = frame.sort_values(["data_type", "ts_code", "trade_date"])

    rows: list[dict] = []
    for (_data_type, _ts_code), group in frame.groupby(["data_type", "ts_code"], sort=False):
        records = group.to_dict("records")
        for start in range(0, len(records), size):
            block = records[start : start + size]
            first, last = block[0], block[-1]
            pre_close = None
            if rows and rows[-1]["ts_code"] == first["ts_code"] and rows[-1]["data_type"] == first["data_type"]:
                pre_close = rows[-1]["close"]
            close = last.get("close")
            row = {
                "ts_code": first["ts_code"],
                "trade_date": last["trade_date"],
                "open": first.get("open"),
                "high": max(b["high"] for b in block if b.get("high") == b["high"]) if any(
                    b.get("high") == b["high"] for b in block
                ) else None,
                "low": min(b["low"] for b in block if b.get("low") == b["low"]) if any(
                    b.get("low") == b["low"] for b in block
                ) else None,
                "close": close,
                "pre_close": pre_close,
                "pct_chg": round((close / pre_close - 1.0) * 100.0, 4)
                if pre_close not in (None, 0) and close == close
                else None,
                "vol": sum(b.get("vol") or 0 for b in block),
                "amount": sum(b.get("amount") or 0 for b in block),
                "adj_factor": last.get("adj_factor"),
                "name": last.get("name"),
                "data_type": first["data_type"],
                "timeframe": f"{size}d",
                "change": close - pre_close
                if pre_close is not None and close == close and pre_close == pre_close
                else None,
            }
            rows.append(row)
    return pd.DataFrame(rows)


__all__ = [
    "KLINE_DEDUPE_KEYS",
    "KLINE_TABLE",
    "derive_multi_day_bars",
    "pack_kline",
]
