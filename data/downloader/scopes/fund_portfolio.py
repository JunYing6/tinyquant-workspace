"""基金持仓（前十大重仓）获取器（旧作用域 ``fund/portfolio``）。

按报告期调用厂商 ``fund_portfolio`` 接口，使用 offset 分页（每页 8000 行），将原始行写入
``{root}/fund_portfolio.parquet``（全局表，按 ``fund_code``+``end_date``+``ts_code`` 去重）。

旧布局将厂商列 ``ts_code``（基金）重命名为 ``fund_code``，将 ``symbol``（所持股票）重命名为
``ts_code``——该重命名复现的是磁盘上的契约（已对照真实卷验证），并非语义映射。
``mkv``/``amount`` 保持厂商的万元单位。
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from data.downloader.base import quarter_end_dates, retry_call

logger = logging.getLogger(__name__)

KEY_FIELDS = (
    "ts_code,ann_date,end_date,symbol,mkv,amount,"
    "stk_mkv_ratio,stk_float_ratio"
)

PAGE_LIMIT = 8000


def fetch_fund_portfolio(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.3,
    **options: Any,
) -> pd.DataFrame:
    """按区间内的每个报告期拉取基金前十大重仓股行。"""
    quarters = quarter_end_dates(start, end)
    logger.info("[fund_portfolio] 报告期: %d 个", len(quarters))
    max_requests = options.get("max_requests", 180)
    max_retries = options.get("max_retries", 3)

    all_rows: list[pd.DataFrame] = []
    for q_end in quarters:
        logger.info("[fund_portfolio] 拉取报告期 %s ...", q_end)

        pages: list[pd.DataFrame] = []
        offset = 0
        request_count = 0
        while request_count < max_requests:
            df = retry_call(
                client.fund_portfolio,
                period=q_end,
                offset=offset,
                limit=PAGE_LIMIT,
                fields=KEY_FIELDS,
                logger=logger,
                label=f"[fund_portfolio] {q_end} offset={offset}",
                max_retries=max_retries,
            )
            if df is None or df.empty:
                break
            pages.append(df)
            request_count += 1
            if len(df) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT
            if sleep > 0:
                time.sleep(0.2)

        if not pages:
            continue

        combined = pd.concat(pages, ignore_index=True)
        # 磁盘上的旧版契约：基金是 fund_code，所持股票是 ts_code
        rename_map = {}
        if "ts_code" in combined.columns:
            rename_map["ts_code"] = "fund_code"
        if "symbol" in combined.columns:
            rename_map["symbol"] = "ts_code"
        if rename_map:
            combined = combined.rename(columns=rename_map)
        combined = combined.drop_duplicates(subset=["fund_code", "end_date", "ts_code"], keep="last")
        all_rows.append(combined)
        logger.info("[fund_portfolio] %s 合并后 %d 行", q_end, len(combined))
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[fund_portfolio] 全期无数据")
        return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)
