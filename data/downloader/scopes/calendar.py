"""Trading calendar fetcher (legacy scope ``calendar/trade_cal``).

Calls the vendor ``trade_cal`` interface and lands the raw frame at
``{root}/trade_date.parquet`` (columns ``cal_date``/``is_open``), the calendar
table every other scope and the workspace adapters rely on.  This scope doubles
as the default trading-date source for scopes that iterate trading days when
no calendar provider is injected.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

KEY_FIELDS = "cal_date,is_open"


def fetch_trade_cal(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.0,
    **options: Any,
) -> pd.DataFrame:
    """Fetch the exchange calendar for the inclusive YYYYMMDD range (vendor-native)."""
    df = client.trade_cal(
        exchange=options.get("exchange", "SSE"),
        start_date=start,
        end_date=end,
        fields=KEY_FIELDS,
    )
    if df is None or df.empty:
        logger.warning("[trade_cal] %s~%s 无数据", start, end)
        return pd.DataFrame()
    # vendor returns string dates / '0'-'1' flags; keep them string-typed
    df = df.copy()
    df["cal_date"] = df["cal_date"].astype(str)
    df["is_open"] = df["is_open"].astype(str)
    return df.sort_values("cal_date").reset_index(drop=True)


def fetch_vendor_trade_dates(client: Any, start: str, end: str) -> list[str]:
    """Resolve open trading dates via the vendor calendar (default calendar source)."""
    df = fetch_trade_cal(client, start, end)
    if df.empty:
        raise ValueError(
            f"vendor trade_cal returned no dates for {start}~{end}; inject a "
            "trade_dates_provider into TushareDownloader to supply a calendar"
        )
    open_days = df[df["is_open"].isin(("1", "1.0", "True", "true"))]
    return open_days["cal_date"].tolist()


def naive_calendar_days(start: str, end: str) -> list[str]:
    """Every calendar day in the range (fallback when no calendar source exists).

    Vendor per-day interfaces return empty frames for non-trading days, so the
    extra calls are wasted but harmless; inject a trade_dates_provider or use
    a client with ``trade_cal`` for efficient trading-day iteration.
    """
    from datetime import date, timedelta

    start_day = date(int(start[:4]), int(start[4:6]), int(start[6:8]))
    end_day = date(int(end[:4]), int(end[4:6]), int(end[6:8]))
    days: list[str] = []
    current = start_day
    while current <= end_day:
        days.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return days
