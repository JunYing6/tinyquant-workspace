"""Per-stock margin detail fetcher (legacy scope ``trade_data/margin_detail``).

Calls the vendor ``margin_detail`` interface per month with offset pagination
(5000 rows per page), keeps the raw vendor columns and lands them at
``{year}/margin.parquet`` with ``level='detail'`` — the per-stock half of the
same file the summary scope writes.  Amounts/quantities stay in vendor yuan.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from data.downloader.base import get_month_range, iter_months, retry_call

logger = logging.getLogger(__name__)

# Tushare margin_detail 接口单次最大返回行数
PAGE_LIMIT = 5000

KEEP_COLUMNS = (
    "trade_date", "ts_code", "rzymye", "rzmre", "rzche",
    "rqmcl", "rqchl", "rqye", "rzrqye",
)


def fetch_margin_detail(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.0,
    **options: Any,
) -> pd.DataFrame:
    """Fetch per-stock margin detail rows for every month in the range."""
    max_retries = options.get("max_retries", 3)
    all_rows: list[pd.DataFrame] = []

    for ym in iter_months(start, end):
        month_start, month_end = get_month_range(ym, end)

        offset = 0
        while True:
            df = retry_call(
                client.margin_detail,
                start_date=month_start,
                end_date=month_end,
                offset=offset,
                logger=logger,
                label=f"[margin_detail] {ym} offset={offset}",
                max_retries=max_retries,
            )
            if df is None or df.empty:
                break

            available = [c for c in KEEP_COLUMNS if c in df.columns]
            page = df[available].copy()
            for col in available:
                if col not in ("trade_date", "ts_code"):
                    page[col] = pd.to_numeric(page[col], errors="coerce").fillna(0)
            all_rows.append(page)

            if len(page) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT
            if sleep > 0:
                time.sleep(0.2)

        logger.info("[margin_detail] %s 完成", ym)

    if not all_rows:
        logger.warning("[margin_detail] 无数据")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result["trade_date"] = result["trade_date"].astype(str)
    result["level"] = "detail"
    return result
