"""日线 K 线 + 日频指标获取器（旧作用域 ``trade_data/daily``）。

旧工作区从八个厂商接口拉取每个交易日的合并表（K 线 + 状态标志 + 估值字段）；
磁盘布局将其拆为两个年度文件，本获取器以字典形式返回同样拆分后的结果，
使两半数据分别落入其映射的布局：

- ``market.bar``          -> ``{year}/kline.parquet``
  （``ts_code, trade_date, open, high, low, close, pre_close, pct_chg, vol,
  amount, adj_factor, name, data_type, timeframe, change`` —— 厂商字段
  外加两个布局标签 ``data_type='stock'``/``timeframe='1d'``）
- ``market.daily_metric`` -> ``{year}/daily_basic.parquet``
  （``is_st``/``is_suspended``/``limit`` 标志及 ``daily_basic``
  估值字段，厂商原生）

单位换算（手->股、千元->元）发生在适配层。
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
    """按交易日拉取厂商表；返回 kline/daily_basic 两个表。"""
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
            time.sleep(max(sleep, 0.5))  # 厂商限流：20 次调用为一组突发
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
        """可选增强接口：当客户端缺少该方法时静默跳过。"""
        fn = getattr(client, method, None)
        if fn is None:
            return pd.DataFrame()
        return call(fn, label, **kwargs)

    # ---- kline 一半：K 线 + 名称 + 复权因子 -----------------------
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

    # ---- daily_basic 一半：状态标志 + 估值字段 ------------------
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
    """填充交易日期，按旧版列序重新索引，保留厂商值。"""
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
