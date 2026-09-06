"""Income statement fetcher (legacy scope ``fina/income_report``).

Calls the vendor ``income_vip`` interface per report period with offset
pagination and lands the raw rows at ``{root}/income_report.parquet`` (global
table, deduplicated on ``ts_code``+``end_date``+``ann_date``).  Amounts stay
in vendor yuan; ``f_ann_date``-vs-``ann_date`` PIT fallback is an adapter rule.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from data.downloader.base import fetch_with_pagination, report_periods

logger = logging.getLogger(__name__)

KEY_FIELDS = (
    "ts_code,end_date,ann_date,"
    "total_revenue,revenue,total_cogs,"
    "operate_profit,total_profit,"
    "n_income_attr_p,n_income,"
    "n_valuechg_p,income_tax,"
    "biz_tax_surchg,"
    "sell_exp,admin_exp,fin_exp,rd_exp,"
    "operate_cost,oper_exp,"
    "int_income,comm_income,n_commis_income,"
    "invest_income,ass_invest_income,"
    "assets_impair_loss,"
    "non_oper_income,non_oper_exp,"
    "basic_eps,diluted_eps"
)


def fetch_income(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.3,
    **options: Any,
) -> pd.DataFrame:
    """Fetch quarterly income statements for every report period in the range."""
    periods = report_periods(start, end)
    logger.info("[income] 报告期列表: %s", periods)

    all_rows: list[pd.DataFrame] = []
    for period in periods:
        logger.info("[income] 拉取 %s ...", period)
        df = fetch_with_pagination(
            func=client.income_vip,
            params={"period": period, "fields": KEY_FIELDS},
            page_size=options.get("page_size", 5000),
            sleep_between=sleep,
            label=f"income {period}",
        )
        if df.empty:
            logger.warning("[income] %s 无数据", period)
            continue
        all_rows.append(df)
        logger.info("[income] %s 拉到 %d 行", period, len(df))

    if not all_rows:
        logger.warning("[income] 全期无数据")
        return pd.DataFrame()

    new_df = pd.concat(all_rows, ignore_index=True)
    if "ann_date" not in new_df.columns:
        raise ValueError("income 数据缺少 ann_date 字段，无法保留公告修订版本")
    return new_df
