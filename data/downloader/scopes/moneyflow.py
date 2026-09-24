"""资金流向获取器（旧作用域 ``trade_data/moneyflow``）。

按每个交易日调用厂商 ``moneyflow`` 接口，将原始行写入 ``{year}/moneyflow.parquet``
（按 ``trade_date``+``ts_code`` 去重）。厂商字段保持原样。

注意：旧下载器额外派生了 ``total_amount``/``net_pct_main`` 列；这种增强属于二阶聚合，
现归处理层所有，因此下载器只写厂商列（``market.money_flow`` 映射也只声明厂商字段）。
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import pandas as pd

from data.downloader.base import retry_call

logger = logging.getLogger(__name__)


def fetch_moneyflow(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.0,
    calendar: Callable[[str, str], list[str]] | None = None,
    **options: Any,
) -> pd.DataFrame:
    """按区间内的每个交易日拉取个股资金流向行。"""
    if calendar is None:
        raise ValueError("trade_data/moneyflow needs a calendar provider (injected or vendor)")
    max_retries = options.get("max_retries", 3)

    frames: list[pd.DataFrame] = []
    for trade_date in calendar(start, end):
        df = retry_call(
            client.moneyflow,
            trade_date=trade_date,
            logger=logger,
            label=f"[moneyflow] {trade_date}",
            max_retries=max_retries,
        )
        if df is None or df.empty:
            continue
        frames.append(df)
        logger.info("[moneyflow] %s 获取 %d 条个股数据", trade_date, len(df))

    if not frames:
        logger.warning("[moneyflow] 无数据")
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    if "trade_date" in result.columns:
        result["trade_date"] = result["trade_date"].astype(str)
    return result
