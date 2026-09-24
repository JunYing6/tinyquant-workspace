"""大宗交易获取器（旧作用域 ``event/block_trade``）。

按每个交易日调用厂商 ``block_trade`` 接口一次，将原始行写入
``{year}/block_trade.parquet``（按 ``ts_code``+``trade_date`` 去重）。
``vol`` 保留厂商的手数单位，``amount`` 保留厂商的千元单位；单位换算放在适配层。
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

import pandas as pd

from data.downloader.base import retry_call

logger = logging.getLogger(__name__)


def fetch_block_trade(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.1,
    calendar: Callable[[str, str], list[str]] | None = None,
    **options: Any,
) -> pd.DataFrame:
    """按区间内的每个交易日拉取大宗交易数据。"""
    if calendar is None:
        raise ValueError("event/block_trade needs a calendar provider (injected or vendor)")
    max_retries = options.get("max_retries", 3)

    all_rows: list[pd.DataFrame] = []
    for td in calendar(start, end):
        df = retry_call(
            client.block_trade,
            trade_date=td,
            logger=logger,
            label=f"[block_trade] {td}",
            max_retries=max_retries,
        )
        if df is None or df.empty:
            continue
        all_rows.append(df)
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[block_trade] 无数据")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result["trade_date"] = result["trade_date"].astype(str)
    return result
