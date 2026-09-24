"""资产负债表获取器（旧作用域 ``fina/balance_report``）。

按报告期调用厂商 ``balancesheet_vip`` 接口，使用 offset 分页，将原始行写入
``{root}/balance_report.parquet``（全局表，按 ``ts_code``+``end_date``+``ann_date`` 去重）。
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from data.downloader.base import fetch_with_pagination, report_periods

logger = logging.getLogger(__name__)

KEY_FIELDS = (
    "ts_code,end_date,ann_date,"
    "total_assets,total_liab,"
    "total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,"
    "total_cur_assets,total_cur_liab,"
    "total_nca,total_ncl,"
    "money_cap,notes_receiv,accounts_receiv,inventories,"
    "fix_assets,intan_assets,goodwill,"
    "lt_rec,defer_tax_assets,"
    "lt_borr,st_borr,"
    "notes_payable,accounts_payable,"
    "adv_receipts,payroll_payable,taxes_payable,int_payable,oth_payable,oth_cur_liab,"
    "defer_tax_liab,"
    "total_share,minority_int"
)


def fetch_balance(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.3,
    **options: Any,
) -> pd.DataFrame:
    """按区间内的每个报告期拉取季度资产负债表。"""
    periods = report_periods(start, end)
    logger.info("[balance] 报告期列表: %s", periods)

    all_rows: list[pd.DataFrame] = []
    for period in periods:
        logger.info("[balance] 拉取 %s ...", period)
        df = fetch_with_pagination(
            func=client.balancesheet_vip,
            params={"period": period, "fields": KEY_FIELDS},
            page_size=options.get("page_size", 5000),
            sleep_between=sleep,
            label=f"balance {period}",
        )
        if df.empty:
            logger.warning("[balance] %s 无数据", period)
            continue
        all_rows.append(df)
        logger.info("[balance] %s 拉到 %d 行", period, len(df))

    if not all_rows:
        logger.warning("[balance] 全期无数据")
        return pd.DataFrame()

    new_df = pd.concat(all_rows, ignore_index=True)
    if "ann_date" not in new_df.columns:
        raise ValueError("balance 数据缺少 ann_date 字段，无法保留公告修订版本")
    return new_df
