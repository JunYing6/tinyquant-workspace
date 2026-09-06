"""ETF/fund daily bars fetcher (legacy scope ``fund/daily``).

Calls the vendor ``fund_daily`` interface per ETF code and lands the rows into
``{year}/kline.parquet`` tagged ``data_type='fund'`` (the ``fund.bar`` dataset
is a filtered view of the shared kline file).  Vendor fields stay raw; the
lots/thousand-yuan conversions happen in the adapters.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from data.downloader.base import retry_call

logger = logging.getLogger(__name__)

DEFAULT_ETF_CODES = [
    "510300.SH",
    "510500.SH",
    "510050.SH",
    "512880.SH",
    "512010.SH",
    "588000.SH",
    "159915.SZ",
    "512800.SH",
    "512690.SH",
    "515790.SH",
    "159928.SZ",
]

FUND_DAILY_FIELDS = (
    "ts_code,trade_date,pre_close,open,high,low,close,pct_chg,vol,amount"
)


def fetch_fund_daily(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.15,
    **options: Any,
) -> pd.DataFrame:
    """Fetch daily bars for the configured ETF codes into the kline layout."""
    etf_codes = options.get("etf_codes") or DEFAULT_ETF_CODES
    max_retries = options.get("max_retries", 3)
    if not etf_codes:
        logger.error("[fund_daily] 无ETF代码")
        return pd.DataFrame()

    all_rows: list[pd.DataFrame] = []
    for code in etf_codes:
        df = retry_call(
            client.fund_daily,
            ts_code=code,
            start_date=start,
            end_date=end,
            fields=FUND_DAILY_FIELDS,
            logger=logger,
            label=f"[fund_daily] {code}",
            max_retries=max_retries,
        )
        if df is None or df.empty:
            logger.warning("[fund_daily] %s 无数据", code)
            continue

        df = df.copy()
        df["trade_date"] = df["trade_date"].astype(str)
        df = df.sort_values("trade_date").reset_index(drop=True)
        all_rows.append(df)
        logger.info("[fund_daily] %s 拉到 %d 行", code, len(df))
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[fund_daily] 无数据")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result["data_type"] = "fund"
    result["timeframe"] = "1d"
    return result
