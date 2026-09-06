"""Market-level margin summary fetcher (legacy scope ``idx=daily_margin``).

Calls the vendor ``margin`` interface per month and aggregates the raw rows to
one summary row per trading day (sums of ``rzmre``/``rzye``/``rqye``/
``rzrqye``), landing them at ``{year}/margin.parquet`` with
``level='summary'``.  The per-stock ``level='detail'`` rows of the same file
are written by the ``trade_data/margin_detail`` scope; the ``market.margin``
adapter filters on ``level``.  Amounts stay in vendor yuan.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from data.downloader.base import get_month_range, iter_months, retry_call

logger = logging.getLogger(__name__)


def fetch_margin(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.0,
    **options: Any,
) -> pd.DataFrame:
    """Fetch exchange-level margin balances and reduce to daily summary rows."""
    max_retries = options.get("max_retries", 3)
    rows: list[dict[str, Any]] = []

    for ym in iter_months(start, end):
        month_start, month_end = get_month_range(ym, end)
        df = retry_call(
            client.margin,
            start_date=month_start,
            end_date=month_end,
            logger=logger,
            label=f"[margin] {ym}",
            max_retries=max_retries,
        )
        if df is None or df.empty:
            logger.warning("[margin] %s 无数据", ym)
            continue

        daily_agg = df.groupby("trade_date").agg(
            rzmre=("rzmre", "sum"),
            rzye=("rzye", "sum"),
            rqye=("rqye", "sum"),
        ).reset_index()
        if "rzrqye" in df.columns:
            rzrqye_agg = df.groupby("trade_date")["rzrqye"].sum().reset_index()
            daily_agg = daily_agg.merge(rzrqye_agg, on="trade_date", how="left")

        for _, r in daily_agg.iterrows():
            rows.append(
                {
                    "trade_date": str(r["trade_date"]),
                    "rzmre": float(r["rzmre"]),
                    "rzye": float(r["rzye"]),
                    "rqye": float(r["rqye"]),
                    "rzrqye": float(r.get("rzrqye", 0)),
                }
            )
        logger.info("[margin] %s 汇总 %d 个交易日", ym, len(daily_agg))
        if sleep > 0:
            time.sleep(sleep)

    if not rows:
        logger.warning("[margin] 无数据")
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result["trade_date"] = result["trade_date"].astype(str)
    result["level"] = "summary"
    result["ts_code"] = None
    return result
