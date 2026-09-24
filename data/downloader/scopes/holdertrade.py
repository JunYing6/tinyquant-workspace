"""股东持股变动获取器（旧作用域 ``event/holdertrade``）。

按月调用厂商 ``stk_holdertrade`` 接口，使用 offset 分页，将原始行写入
``{root}/holdertrade.parquet``（全局表，按 ``ts_code``+``ann_date``+``holder_name``+``in_de`` 去重）。
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from data.downloader.base import fetch_with_pagination, get_month_range, iter_months

logger = logging.getLogger(__name__)

KEY_FIELDS = (
    "ts_code,ann_date,holder_name,holder_type,in_de,"
    "change_vol,change_ratio,after_share,after_ratio,"
    "avg_price,total_share,begin_date,close_date"
)


def fetch_holdertrade(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.3,
    **options: Any,
) -> pd.DataFrame:
    """按区间内的每个月拉取股东持股变动行。"""
    all_rows: list[pd.DataFrame] = []

    for ym in iter_months(start, end):
        month_start, month_end = get_month_range(ym, end)
        logger.info("[holdertrade] 拉取 %s ~ %s ...", month_start, month_end)
        df = fetch_with_pagination(
            func=client.stk_holdertrade,
            params={"start_date": month_start, "end_date": month_end, "fields": KEY_FIELDS},
            page_size=options.get("page_size", 5000),
            sleep_between=sleep,
            label=f"holdertrade {ym}",
        )
        if df.empty:
            logger.debug("[holdertrade] %s~%s 无数据", month_start, month_end)
            continue
        all_rows.append(df)
        logger.info("[holdertrade] %s~%s 拉到 %d 行", month_start, month_end, len(df))
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[holdertrade] 全期无数据")
        return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)
