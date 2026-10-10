"""Kline 打包：按日 bar 文件 -> 年度 ``kline`` duckdb 表。

日线类 scope（``trade_data/daily``、``index/daily``、``fund/daily``）
将每个交易日的厂商行落到 ``{year}/{MMDD}/kline.parquet``——即原始层
（该文件为统一 kline schema，``timeframe`` 列区分 1d 与日内分钟；
``prune_per_day_1d`` 后 per-day 只保留日内分钟，1d 权威在 ``year.duckdb#kline``）。
本模块将这些 1d 行打包进 ``{year}/year.duckdb`` 的 ``kline`` 表
（``market.bar`` 适配器读取的布局），并从 1d 行派生出多日周期（5d/10d）：

- 对每个标的（``data_type`` + ``ts_code``），将当年按 ``trade_date``
  排序的 1d 行切分成连续每 N 个交易日的分组；
- 每个分组聚合成一行：open = 首日开盘，high/low = 极值，
  close = 末日收盘，vol/amount = 求和，``trade_date`` = 分组最后一天，
  ``adj_factor``/``name`` = 分组最后一行，``pre_close`` = 前一组的收盘
  （首组为 NaN），``pct_chg``/``change`` 重新计算；
- ``timeframe`` 为 ``"5d"``/``"10d"``。

打包是幂等的：1d 行从日线文件合并而来（业务键
``ts_code``+``trade_date``+``data_type``+``timeframe``，保留最后一条），
派生周期从头重新计算，表整体原子替换。
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
    """将 ``{root}/{year}/{MMDD}/kline.parquet`` 打包进 ``{root}/{year}/year.duckdb``。

    返回 duckdb 路径。可随时安全重跑；1d 行从日线文件合并，
    多日行重新计算。
    """
    year_dir = Path(root) / str(year)
    db_path = year_dir / "year.duckdb"

    day_files = sorted(year_dir.glob("*/kline.parquet"))
    frames: list[pd.DataFrame] = []
    for path in day_files:
        try:
            frame = pd.read_parquet(path, filters=[("timeframe", "==", KLINE_BASE_TIMEFRAME)])
        except Exception:
            frame = pd.read_parquet(path)
            if "timeframe" in frame.columns:
                frame = frame[frame["timeframe"] == KLINE_BASE_TIMEFRAME]
        if not frame.empty:
            frames.append(frame)
    file_rows = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

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
    """将已有表中的 1d 行与日线文件行合并（保留最后一条）。"""
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
    """将 1d 行按标的聚合成连续每 N 个交易日的分组。"""
    if base_1d.empty:
        return pd.DataFrame()
    required = {"ts_code", "trade_date", "data_type", "open", "high", "low", "close"}
    missing = required - set(base_1d.columns)
    if missing:
        raise ValueError(f"kline 1d rows missing columns for derivation: {sorted(missing)}")

    frame = base_1d.copy()
    frame["trade_date"] = frame["trade_date"].astype("string")
    frame = frame[frame["trade_date"].notna()]  # 无日期的行无法组成 bar
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
