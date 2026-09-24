"""股票主表获取器（旧作用域 ``stock/basic``）。

调用厂商 ``stock_basic`` 接口获取全部已上市 A 股标的，将原始表写入 ``{root}/stock_basic.parquet``
（全局快照，按 ``ts_code`` 去重）。厂商字段保持原样：``list_status`` 的 L/D/P 解码发生在适配层。
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from data.downloader.base import retry_call

logger = logging.getLogger(__name__)

KEY_FIELDS = (
    "ts_code,symbol,name,area,industry,fullname,market,"
    "exchange,list_status,list_date,delist_date,is_hs"
)


def fetch_stock_basic(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.0,
    **options: Any,
) -> pd.DataFrame:
    """拉取完整的已上市标的主表（start/end 不使用，快照作用域）。"""
    list_status = options.get("list_status", "L")
    df = retry_call(
        client.stock_basic,
        exchange=options.get("exchange", ""),
        list_status=list_status,
        fields=KEY_FIELDS,
        logger=logger,
        label="[stock_basic]",
        max_retries=options.get("max_retries", 3),
    )
    if df is None or df.empty:
        logger.warning("[stock_basic] 无数据 (list_status=%s)", list_status)
        return pd.DataFrame()
    logger.info("[stock_basic] 拉到 %d 行", len(df))
    return df
