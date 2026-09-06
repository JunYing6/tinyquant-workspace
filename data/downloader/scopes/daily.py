"""Daily bars + daily metrics fetcher (legacy scope ``trade_data/daily``).

The legacy workspace fetched one merged frame per trading day (bars + status
flags + valuation fields) from eight vendor interfaces; the on-disk layout
splits it into two yearly files, and this fetcher returns the same split as a
dict so each half lands in its mapped layout:

- ``market.bar``          -> ``{year}/kline.parquet``
  (``ts_code, trade_date, open, high, low, close, pre_close, pct_chg, vol,
  amount, adj_factor, name, data_type, timeframe, change`` — vendor fields
  plus the two layout tags ``data_type='stock'``/``timeframe='1d'``)
- ``market.daily_metric`` -> ``{year}/daily_basic.parquet``
  (``is_st``/``is_suspended``/``limit`` flags and the ``daily_basic``
  valuation fields, vendor-native)

Unit conversions (lots->shares, thousand-yuan->yuan) happen in the adapters.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

import pandas as pd

from data.downloader.base import retry_call

logger = logging.getLogger(__name__)

DAILY_FIELDS = (
    "ts_code,pre_close,trade_date,open,high,low,close,change,pct_chg,vol,amount"
)
DAILY_BASIC_FIELDS = (
    "ts_code,turnover_rate_f,volume_ratio,pe_ttm,pb,ps_ttm,dv_ttm,"
    "total_share,float_share,free_share,total_mv,circ_mv"
)

KLINE_COLUMNS = [
    "ts_code", "trade_date", "open", "high", "low", "close", "pre_close",
    "pct_chg", "vol", "amount", "adj_factor", "name", "data_type", "timeframe",
    "change",
]
BASIC_COLUMNS = [
    "ts_code", "trade_date", "is_st", "is_suspended", "turnover_rate_f",
    "volume_ratio", "pe_ttm", "pb", "ps_ttm", "dv_ttm", "total_share",
    "float_share", "free_share", "total_mv", "circ_mv", "up_limit",
    "down_limit", "limit",
]


def fetch_daily(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.35,
    calendar: Callable[[str, str], list[str]] | None = None,
    **options: Any,
) -> dict[str, pd.DataFrame]:
    """Fetch per-trading-day vendor frames; returns kline/daily_basic frames."""
    if calendar is None:
        raise ValueError("trade_data/daily needs a calendar provider (injected or vendor)")
    trade_dates = calendar(start, end)
    if not trade_dates:
        logger.error("[daily] 无交易日")
        return {"market.bar": pd.DataFrame(), "market.daily_metric": pd.DataFrame()}

    kline_frames: list[pd.DataFrame] = []
    basic_frames: list[pd.DataFrame] = []
    max_retries = options.get("max_retries", 3)

    for index, trade_date in enumerate(trade_dates, 1):
        kline, basic = _download_single_day(client, trade_date, max_retries)
        if kline is not None and not kline.empty:
            kline_frames.append(kline)
        if basic is not None and not basic.empty:
            basic_frames.append(basic)
        if index % 20 == 0 and sleep > 0:
            time.sleep(max(sleep, 0.5))  # vendor rate limit: burst of 20 calls
        elif sleep > 0:
            time.sleep(sleep)

    kline_df = (
        pd.concat(kline_frames, ignore_index=True) if kline_frames else pd.DataFrame()
    )
    basic_df = (
        pd.concat(basic_frames, ignore_index=True) if basic_frames else pd.DataFrame()
    )
    logger.info(
        "[daily] 完成: kline %d 行, daily_basic %d 行 (%d 个交易日)",
        len(kline_df), len(basic_df), len(trade_dates),
    )
    return {"market.bar": kline_df, "market.daily_metric": basic_df}


def _download_single_day(
    client: Any,
    date: str,
    max_retries: int,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    def call(func: Any, label: str, **kwargs: Any) -> pd.DataFrame:
        df = retry_call(func, logger=logger, label=label, max_retries=max_retries, **kwargs)
        return df if df is not None else pd.DataFrame()

    def optional(method: str, label: str, **kwargs: Any) -> pd.DataFrame:
        """Optional enrichment interface: skip silently when the client lacks it."""
        fn = getattr(client, method, None)
        if fn is None:
            return pd.DataFrame()
        return call(fn, label, **kwargs)

    # ---- kline half: bars + names + adjustment factor -----------------------
    kline = call(
        client.daily,
        f"daily {date}",
        trade_date=date,
        fields=DAILY_FIELDS,
    )
    if kline.empty:
        return None, None

    names = optional("bak_basic", f"bak_basic {date}", trade_date=date, fields="ts_code,name")
    if not names.empty:
        kline = kline.merge(names[["ts_code", "name"]], on="ts_code", how="left")

    adj = optional("adj_factor", f"adj_factor {date}", trade_date=date, fields="ts_code,adj_factor")
    if not adj.empty:
        kline = kline.merge(adj, on="ts_code", how="left")

    kline["data_type"] = "stock"
    kline["timeframe"] = "1d"
    kline = _finalize(kline, date, KLINE_COLUMNS)

    # ---- daily_basic half: status flags + valuation fields ------------------
    basic = optional(
        "daily_basic",
        f"daily_basic {date}",
        trade_date=date,
        fields=DAILY_BASIC_FIELDS,
    )
    if basic.empty:
        return kline, None

    st = optional("stock_st", f"stock_st {date}", trade_date=date, fields="ts_code")
    if not st.empty:
        st["is_st"] = 1
        basic = basic.merge(st[["ts_code", "is_st"]], on="ts_code", how="left")

    suspended = optional(
        "suspend_d",
        f"suspend_d {date}",
        suspend_type="S",
        trade_date=date,
        fields="ts_code",
    )
    if not suspended.empty:
        suspended["is_suspended"] = 1
        basic = basic.merge(suspended[["ts_code", "is_suspended"]], on="ts_code", how="left")

    limits = optional("limit_list_d", f"limit_list_d {date}", trade_date=date, fields="ts_code,limit")
    if not limits.empty:
        basic = basic.merge(limits[["ts_code", "limit"]], on="ts_code", how="left")

    price_limits = optional(
        "stk_limit",
        f"stk_limit {date}",
        trade_date=date,
        fields="ts_code,up_limit,down_limit",
    )
    if not price_limits.empty:
        basic = basic.merge(price_limits, on="ts_code", how="left")

    basic = _finalize(basic, date, BASIC_COLUMNS)
    return kline, basic


def _finalize(frame: pd.DataFrame, date: str, columns: list[str]) -> pd.DataFrame:
    """Fill the trade date, reindex to the legacy column order, keep vendor values."""
    frame = frame.copy()
    if "trade_date" in frame.columns:
        frame["trade_date"] = frame["trade_date"].fillna(date)
    else:
        frame["trade_date"] = date
    frame["trade_date"] = frame["trade_date"].astype(str)
    for col in columns:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame[columns]
