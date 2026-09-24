"""股权质押统计获取器（旧作用域 ``event/pledge_stat``）。

按每个交易日调用厂商 ``pledge_stat`` 接口，把查询日期写入 ``trade_date``（该接口返回季度
``end_date`` 但不含其快照日期），将行写入 ``{year}/pledge_stat.parquet``
（按 ``ts_code``+``trade_date`` 去重）。
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
    """按区间内的每个交易日拉取质押统计快照。"""
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
