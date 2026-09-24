"""财务指标获取器（旧作用域 ``fina/indicator``）。

按报告期调用厂商 ``fina_indicator_vip`` 接口，使用 offset 分页，将原始行写入
``{root}/fina_indicator.parquet``（全局表，按 ``ts_code``+``end_date``+``ann_date`` 去重，
因此重新公告的修订版本会被保留）。``ann_date`` 是 PIT 锚点——没有它获取器拒绝写入。
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from data.downloader.base import fetch_with_pagination, report_periods

logger = logging.getLogger(__name__)

KEY_FIELDS = (
    "ts_code,end_date,ann_date,"
    "roe,roe_waa,roe_dt,roe_yearly,"
    "roa,roic,roa2_yearly,"
    "grossprofit_margin,netprofit_margin,op_of_gr,"
    "current_ratio,quick_ratio,cash_ratio,"
    "debt_to_assets,debt_to_eqt,assets_to_eqt,"
    "bps,eps,ocfps,total_revenue_ps,"
    "assets_turn,ar_turn,"
    "or_yoy,netprofit_yoy,profit_dedt,"
    "basic_eps_yoy,dt_eps_yoy"
)


def fetch_fina_indicator(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.3,
    **options: Any,
) -> pd.DataFrame:
    """按区间内的每个报告期拉取季度财务指标。"""
    periods = report_periods(start, end)
    logger.info("[fina] 报告期列表: %s", periods)

    all_rows: list[pd.DataFrame] = []
    for period in periods:
        logger.info("[fina] 拉取 %s ...", period)
        df = fetch_with_pagination(
            func=client.fina_indicator_vip,
            params={"period": period, "fields": KEY_FIELDS},
            page_size=options.get("page_size", 5000),
            sleep_between=sleep,
            label=f"fina {period}",
        )
        if df.empty:
            logger.warning("[fina] %s 无数据", period)
            continue
        all_rows.append(df)
        logger.info("[fina] %s 拉到 %d 行", period, len(df))

    if not all_rows:
        logger.warning("[fina] 全期无数据")
        return pd.DataFrame()

    new_df = pd.concat(all_rows, ignore_index=True)
    if "ann_date" not in new_df.columns:
        raise ValueError("fina 数据缺少 ann_date 字段，无法保留公告修订版本")
    return new_df
