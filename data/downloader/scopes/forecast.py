"""业绩预告获取器（旧作用域 ``event/forecast``）。

按每个交易日调用厂商 ``forecast`` 接口（该接口需要 ``ann_date``），将原始行写入
``{root}/forecast.parquet``（全局表，按 ``ts_code``+``ann_date``+``end_date`` 去重）。

旧卷在该文件中只保留策略关注的预告类型（预减/扭亏），因此保留该行过滤逻辑；
``type`` 列本身是厂商原生值。该接口限流为 120 次/分钟。
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

import pandas as pd

from data.downloader.base import retry_call

logger = logging.getLogger(__name__)

KEY_FIELDS = (
    "ts_code,ann_date,end_date,type,"
    "p_change_min,p_change_max,"
    "net_profit_min,net_profit_max"
)

# 策略关注的预告类型（与真实卷上的 forecast.parquet 一致）
TARGET_TYPES = ["预减", "扭亏"]


def fetch_forecast(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.55,
    calendar: Callable[[str, str], list[str]] | None = None,
    **options: Any,
) -> pd.DataFrame:
    """按区间内每个交易日拉取当日公告的业绩预告。"""
    if calendar is None:
        raise ValueError("event/forecast needs a calendar provider (injected or vendor)")
    trade_dates = calendar(start, end)
    logger.info("[forecast] 交易日期数: %d", len(trade_dates))
    max_retries = options.get("max_retries", 3)
    sleep = options.get("sleep", sleep)

    all_rows: list[pd.DataFrame] = []
    failed_dates: list[str] = []
    for i, date in enumerate(trade_dates, 1):
        if i % 30 == 0:
            logger.info("[forecast] 进度: %d / %d", i, len(trade_dates))
        df = retry_call(
            client.forecast,
            ann_date=date,
            fields=KEY_FIELDS,
            logger=logger,
            label=f"[forecast] {date}",
            max_retries=max_retries,
        )
        if df is None:
            failed_dates.append(date)
            continue
        if df.empty:
            pass
        else:
            all_rows.append(df)
            logger.info("[forecast] %s 拉到 %d 行", date, len(df))
        if sleep > 0:
            time.sleep(sleep)  # forecast 接口限流 120 次/分钟

    if failed_dates:
        logger.warning("[forecast] 最终仍失败的日期: %s", failed_dates)

    if not all_rows:
        logger.warning("[forecast] 全期无数据")
        return pd.DataFrame()

    new_df = pd.concat(all_rows, ignore_index=True)
    logger.info("[forecast] 合计 %d 行（过滤前）", len(new_df))

    if "type" in new_df.columns:
        before = len(new_df)
        new_df = new_df[new_df["type"].isin(TARGET_TYPES)]
        logger.info("[forecast] 类型过滤: %d -> %d 行 (保留 %s)", before, len(new_df), TARGET_TYPES)

    return new_df.drop_duplicates(subset=["ts_code", "ann_date", "end_date"], keep="last")
