"""Block-trade fetcher (legacy scope ``event/block_trade``).

Calls the vendor ``block_trade`` interface once per trading day and lands the
raw rows at ``{year}/block_trade.parquet`` (deduplicated on
``ts_code``+``trade_date``).  ``vol`` stays in vendor lots and ``amount`` in
vendor thousand-yuan; conversions live in the adapters.
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
    """Fetch block trades for every trading day in the range."""
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
