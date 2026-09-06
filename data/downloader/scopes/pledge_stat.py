"""Share-pledge statistics fetcher (legacy scope ``event/pledge_stat``).

Calls the vendor ``pledge_stat`` interface once per trading day, stamps the
query date into ``trade_date`` (the interface reports the quarter ``end_date``
but not its snapshot date) and lands the rows at
``{year}/pledge_stat.parquet`` (deduplicated on ``ts_code``+``trade_date``).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

import pandas as pd

from data.downloader.base import retry_call

logger = logging.getLogger(__name__)


def fetch_pledge_stat(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.1,
    calendar: Callable[[str, str], list[str]] | None = None,
    **options: Any,
) -> pd.DataFrame:
    """Fetch pledge statistics snapshots for every trading day in the range."""
    if calendar is None:
        raise ValueError("event/pledge_stat needs a calendar provider (injected or vendor)")
    max_retries = options.get("max_retries", 3)

    all_rows: list[pd.DataFrame] = []
    for td in calendar(start, end):
        df = retry_call(
            client.pledge_stat,
            trade_date=td,
            logger=logger,
            label=f"[pledge_stat] {td}",
            max_retries=max_retries,
        )
        if df is None or df.empty:
            continue
        df = df.copy()
        df["trade_date"] = td
        all_rows.append(df)
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[pledge_stat] no data")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result["trade_date"] = result["trade_date"].astype(str)
    return result
