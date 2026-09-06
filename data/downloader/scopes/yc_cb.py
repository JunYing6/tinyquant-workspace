"""Government bond yield-curve fetcher (legacy scope ``trade_data/yc_cb``).

Calls the vendor ``yc_cb`` interface in small batches (one call returns only a
few trading days), keeps the requested ``curve_term`` rows and lands them at
``{year}/yc_cb.parquet`` (``trade_date``/``ts_code``/``curve_name``/
``curve_type``/``curve_term``/``yield``, vendor percentage points).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)


def fetch_yc_cb(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.0,
    calendar: Callable[[str, str], list[str]] | None = None,
    **options: Any,
) -> pd.DataFrame:
    """Fetch bond yield-curve rows for the configured curve in trading-day batches."""
    if calendar is None:
        raise ValueError("trade_data/yc_cb needs a calendar provider (injected or vendor)")
    trade_dates = calendar(start, end)
    if not trade_dates:
        logger.error("[yc_cb] 无交易日")
        return pd.DataFrame()

    curve_term = options.get("curve_term", 10.0)
    curve_type = options.get("curve_type", 0)
    ts_code = options.get("ts_code", "1001.CB")
    batch_days = options.get("batch_days", 4)

    rows: list[pd.DataFrame] = []
    for i in range(0, len(trade_dates), batch_days):
        batch = trade_dates[i : i + batch_days]
        try:
            df = client.yc_cb(
                ts_code=ts_code,
                curve_type=curve_type,
                start_date=batch[0],
                end_date=batch[-1],
            )
        except Exception as e:
            logger.error("[yc_cb] %s~%s 请求失败: %s", batch[0], batch[-1], e)
            continue

        if df is None or df.empty:
            logger.warning("[yc_cb] %s~%s 无数据", batch[0], batch[-1])
            continue

        sub = df[df["curve_term"] == curve_term].copy()
        if sub.empty:
            logger.warning("[yc_cb] %s~%s 无 %sY 数据", batch[0], batch[-1], curve_term)
            continue
        rows.append(sub)
        if sleep > 0:
            time.sleep(sleep)

    if not rows:
        logger.warning("[yc_cb] 无数据")
        return pd.DataFrame()

    result = pd.concat(rows, ignore_index=True)
    result["trade_date"] = result["trade_date"].astype(str)
    result["curve_type"] = result["curve_type"].astype(int)
    return result.sort_values("trade_date").reset_index(drop=True)
