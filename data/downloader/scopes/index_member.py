"""Index membership (weights) fetcher (legacy scope ``index/member``).

Calls the vendor ``index_weight`` interface per index code and lands each
trading day's constituent rows in ``{year}/year.duckdb`` table
``index_member`` (``index_code``/``con_code``/``trade_date``/``weight``,
deduplicated on ``index_code``+``con_code``+``trade_date``).  The legacy
per-day ``index_member.parquet`` files are consolidated into the yearly table.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

from data.downloader.base import RawVolumeWriter, merge_duckdb_table, retry_call

logger = logging.getLogger(__name__)

DEFAULT_INDEXES = [
    "000300.SH",  # 沪深300
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "399101.SZ",  # 深证100指数
    "399102.SZ",  # 中小板100指数
]


def fetch_index_member(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.3,
    **options: Any,
) -> pd.DataFrame:
    """Fetch constituent weights for the configured index codes."""
    index_codes = options.get("index_codes") or DEFAULT_INDEXES
    max_retries = options.get("max_retries", 3)

    all_rows: list[pd.DataFrame] = []
    for index_code in index_codes:
        logger.info("[index_member] 下载 %s %s~%s ...", index_code, start, end)
        df = retry_call(
            client.index_weight,
            index_code=index_code,
            start_date=start,
            end_date=end,
            logger=logger,
            label=f"[index_member] {index_code}",
            max_retries=max_retries,
        )
        if df is not None and not df.empty:
            all_rows.append(df)
            logger.info("[index_member] %s 获取 %d 行", index_code, len(df))
        else:
            logger.warning("[index_member] %s 无数据", index_code)
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[index_member] 无数据")
        return pd.DataFrame()
    result = pd.concat(all_rows, ignore_index=True)
    if "trade_date" in result.columns:
        result["trade_date"] = result["trade_date"].astype(str)
    return result


def write_index_member(
    writer: RawVolumeWriter,
    frame: pd.DataFrame,
    **options: Any,
) -> list[Path]:
    """Custom writer: ``{root}/{year}/year.duckdb`` table ``index_member`` by ``trade_date``."""
    if frame.empty:
        return []
    if "trade_date" not in frame.columns:
        raise ValueError(f"index_member rows need trade_date, got {list(frame.columns)}")
    frame = frame.copy()
    frame["trade_date"] = frame["trade_date"].astype(str)
    years = frame["trade_date"].str.slice(0, 4)
    written: list[Path] = []
    for year, group in frame.groupby(years, sort=True):
        db_path = writer.root / str(year) / "year.duckdb"
        merge_duckdb_table(db_path, "index_member", group, ["index_code", "con_code", "trade_date"])
        written.append(db_path)
    return written
