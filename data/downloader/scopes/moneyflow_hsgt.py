"""HSGT flow summary fetcher (legacy scope ``trade_data/moneyflow_hsgt``).

Calls the vendor ``moneyflow_hsgt`` interface per month and lands the raw rows
at ``{year}/moneyflow_hsgt.parquet`` (``ggt_ss``/``ggt_sz``/``hgt``/``sgt``/
``north_money``/``south_money``, vendor yuan).  The per-stock northbound
net-buy file is a different scope (``trade_data/northbound``).
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
    """Fetch HSGT north/south flow summary rows for every month in the range."""
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
