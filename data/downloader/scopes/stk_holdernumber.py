"""股东户数获取器（旧作用域 ``event/stk_holdernumber``）。

按月调用厂商 ``stk_holdernumber`` 接口，使用 offset 分页，将原始行写入
``{year}/stk_holdernumber.parquet``（按 ``ann_date`` 的年份分组，按 ``ts_code``+``ann_date`` 去重）。
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from data.downloader.base import fetch_with_pagination, get_month_range, iter_months

logger = logging.getLogger(__name__)


def fetch_stk_holdernumber(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.2,
    **options: Any,
) -> pd.DataFrame:
    """按区间内的每个月拉取股东户数行。"""
    all_rows: list[pd.DataFrame] = []

    for ym in iter_months(start, end):
        month_start, month_end = get_month_range(ym, end)
        df = fetch_with_pagination(
            func=client.stk_holdernumber,
            params={"start_date": month_start, "end_date": month_end},
            page_size=options.get("page_size", 5000),
            sleep_between=sleep,
            label=f"stk_holdernumber {ym}",
        )
        if df.empty:
            continue
        all_rows.append(df)
        logger.info("[stk_holdernumber] %s 获取 %d 行", ym, len(df))

    if not all_rows:
        logger.warning("[stk_holdernumber] 无数据")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result["ann_date"] = result["ann_date"].astype(str)
    return result
