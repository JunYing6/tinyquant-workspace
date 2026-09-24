"""SHIBOR 获取器（旧作用域 ``trade_data/shibor``）。

按月调用厂商 ``shibor`` 接口，将原始宽表写入 ``{year}/shibor.parquet``（``date`` 及
``on``/``1w``/.../``1y`` 各期限，厂商百分点单位）。宽表转长表（observation_date, term, rate）的
重构在 ``market.shibor`` 映射中声明，并发生在适配层。
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from data.downloader.base import get_month_range, iter_months, retry_call

logger = logging.getLogger(__name__)


def fetch_shibor(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.0,
    **options: Any,
) -> pd.DataFrame:
    """按区间内的每个月拉取 SHIBOR 报价。"""
    max_retries = options.get("max_retries", 3)
    all_rows: list[pd.DataFrame] = []

    for ym in iter_months(start, end):
        month_start, month_end = get_month_range(ym, end)
        df = retry_call(
            client.shibor,
            start_date=month_start,
            end_date=month_end,
            logger=logger,
            label=f"[shibor] {ym}",
            max_retries=max_retries,
        )
        if df is None or df.empty:
            continue
        all_rows.append(df)
        logger.info("[shibor] %s 获取 %d 行", ym, len(df))
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[shibor] 无数据")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result["date"] = result["date"].astype(str)
    return result
