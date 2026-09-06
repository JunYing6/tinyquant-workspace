"""Moneyflow fetcher (legacy scope ``trade_data/moneyflow``).

Calls the vendor ``moneyflow`` interface per trading day and lands the raw
rows at ``{year}/moneyflow.parquet`` (deduplicated on ``trade_date`` +
``ts_code``).  Vendor fields stay raw.

Note: the legacy downloader additionally derived ``total_amount`` /
``net_pct_main`` columns; that enrichment is a second-order aggregation and now
belongs to the processing layer, so the downloader writes the vendor columns
only (the ``market.money_flow`` mapping declares just the vendor fields).
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
    """Fetch per-stock moneyflow rows for every trading day in the range."""
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
