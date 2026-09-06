"""Holder-number fetcher (legacy scope ``event/stk_holdernumber``).

Calls the vendor ``stk_holdernumber`` interface per month with offset
pagination and lands the raw rows at ``{year}/stk_holdernumber.parquet``
(grouped by ``ann_date`` year, deduplicated on ``ts_code``+``ann_date``).
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
    """Fetch holder-number rows for every month in the range."""
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
