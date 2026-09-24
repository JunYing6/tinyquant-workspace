"""指数日线获取器（旧作用域 ``index/daily``）。

按每个指数代码调用厂商 ``index_daily`` 接口，将行写入 ``{year}/kline.parquet`` 并打上
``data_type='index'`` 标签——与股票 K 线是同一个物理文件；``index.bar`` 数据集是它的过滤视图。
厂商字段保持原样（``vol`` 为指数原生单位，``amount`` 为千元）；单位换算在适配层。
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from data.downloader.base import retry_call

logger = logging.getLogger(__name__)

DEFAULT_INDEX_CODES = [
    "000300.SH",
    "000001.SH",
    "399001.SZ",
    "399006.SZ",
]


def fetch_index_daily(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.0,
    **options: Any,
) -> pd.DataFrame:
    """将配置的指数代码的日线数据拉取到 K 线布局。"""
    index_codes = options.get("index_codes") or DEFAULT_INDEX_CODES
    max_retries = options.get("max_retries", 3)

    all_rows: list[pd.DataFrame] = []
    for code in index_codes:
        df = retry_call(
            client.index_daily,
            ts_code=code,
            start_date=start,
            end_date=end,
            logger=logger,
            label=f"[index] {code}",
            max_retries=max_retries,
        )
        if df is None or df.empty:
            logger.warning("[index] %s 无数据", code)
            continue

        df = df.copy()
        df["trade_date"] = df["trade_date"].astype(str)
        df = df.sort_values("trade_date").reset_index(drop=True)
        all_rows.append(df)
        logger.info("[index] %s 拉到 %d 行", code, len(df))
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[index] 无数据")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result["data_type"] = "index"
    result["timeframe"] = "1d"
    return result
