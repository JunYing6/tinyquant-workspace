"""沪深港通资金流汇总获取器（旧作用域 ``trade_data/moneyflow_hsgt``）。

按月调用厂商 ``moneyflow_hsgt`` 接口，将原始行写入 ``{year}/moneyflow_hsgt.parquet``
（``ggt_ss``/``ggt_sz``/``hgt``/``sgt``/``north_money``/``south_money``，厂商元单位）。
按个股的北向净买入文件是另一个作用域（``trade_data/northbound``）。
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from data.downloader.base import get_month_range, iter_months, retry_call

logger = logging.getLogger(__name__)


def fetch_moneyflow_hsgt(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.0,
    **options: Any,
) -> pd.DataFrame:
    """按区间内的每个月拉取沪深港通南北向资金流汇总行。"""
    max_retries = options.get("max_retries", 3)
    all_rows: list[pd.DataFrame] = []

    for ym in iter_months(start, end):
        month_start, month_end = get_month_range(ym, end)
        df = retry_call(
            client.moneyflow_hsgt,
            start_date=month_start,
            end_date=month_end,
            logger=logger,
            label=f"[moneyflow_hsgt] {ym}",
            max_retries=max_retries,
        )
        if df is None or df.empty:
            continue
        all_rows.append(df)
        logger.info("[moneyflow_hsgt] %s 获取 %d 行", ym, len(df))
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[moneyflow_hsgt] 无数据")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result["trade_date"] = result["trade_date"].astype(str)
    return result
