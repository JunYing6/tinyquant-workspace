"""指数成分（权重）获取器（旧作用域 ``index/member``）。

按每个指数代码调用厂商 ``index_weight`` 接口，将每个交易日的成分股行写入
``{year}/year.duckdb`` 表的 ``index_member``（``index_code``/``con_code``/``trade_date``/``weight``，
按 ``index_code``+``con_code``+``trade_date`` 去重）。旧版按日的 ``index_member.parquet`` 文件
被合并进年度表。
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
    """拉取配置的指数代码的成分股权重。"""
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
    """自定义写入器：按 ``trade_date`` 写入 ``{root}/{year}/year.duckdb`` 表的 ``index_member``。"""
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
