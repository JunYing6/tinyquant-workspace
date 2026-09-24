"""市场级融资融券汇总获取器（旧作用域 ``idx=daily_margin``）。

按月调用厂商 ``margin`` 接口，将原始行聚合成每个交易日的汇总行（``rzmre``/``rzye``/``rqye``/
``rzrqye`` 的求和），写入 ``{year}/margin.parquet``，带 ``level='summary'``。同一文件中按个股的
``level='detail'`` 行由 ``trade_data/margin_detail`` 作用域写入；``market.margin`` 适配器按
``level`` 过滤。金额保持厂商的元单位。
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
    """拉取交易所级融资融券余额，并降维为每日汇总行。"""
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
