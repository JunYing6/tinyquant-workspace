"""交易日历获取器（旧作用域 ``calendar/trade_cal``）。

调用厂商 ``trade_cal`` 接口，将原始表写入 ``{root}/trade_date.parquet``
（列为 ``cal_date``/``is_open``），这是其他作用域和工作区适配器依赖的日历表。
当未注入日历提供者时，本作用域兼作按交易日迭代的作用域的默认交易日来源。
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
    """拉取闭区间 YYYYMMDD 范围内的交易所日历（厂商原生接口）。"""
    df = client.trade_cal(
        exchange=options.get("exchange", "SSE"),
        start_date=start,
        end_date=end,
        fields=KEY_FIELDS,
    )
    if df is None or df.empty:
        logger.warning("[trade_cal] %s~%s 无数据", start, end)
        return pd.DataFrame()
    # 厂商返回字符串日期 / '0'-'1' 标志；保持其字符串类型
    df = df.copy()
    df["cal_date"] = df["cal_date"].astype(str)
    df["is_open"] = df["is_open"].astype(str)
    return df.sort_values("cal_date").reset_index(drop=True)


def fetch_vendor_trade_dates(client: Any, start: str, end: str) -> list[str]:
    """通过厂商日历解析开放交易日（默认日历来源）。"""
    df = fetch_trade_cal(client, start, end)
    if df.empty:
        raise ValueError(
            f"vendor trade_cal returned no dates for {start}~{end}; inject a "
            "trade_dates_provider into TushareDownloader to supply a calendar"
        )
    open_days = df[df["is_open"].isin(("1", "1.0", "True", "true"))]
    return open_days["cal_date"].tolist()


def naive_calendar_days(start: str, end: str) -> list[str]:
    """区间内的每个日历日（当不存在日历来源时的回退方案）。

    厂商按日接口在非交易日会返回空表，多出的调用虽有浪费但无害；可注入
    trade_dates_provider，或使用带 ``trade_cal`` 的客户端以高效地按交易日迭代。
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
