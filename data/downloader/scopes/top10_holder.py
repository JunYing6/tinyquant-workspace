"""Top-10 float holders fetcher (legacy scope ``event/top10_holder``).

Walks every listed instrument (from the vendor ``stock_basic`` interface) x
every report quarter and calls ``top10_floatholders`` per pair, landing the
raw rows at ``{root}/top10_holder.parquet`` (global table, deduplicated on
``ts_code``+``end_date``+``holder_name``).  The legacy temp-file/resume
machinery collapses into the writer's merge-dedupe: re-running a range
refreshes existing rows instead of duplicating them.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from data.downloader.base import quarter_end_dates, retry_call

logger = logging.getLogger(__name__)

KEY_FIELDS = "ts_code,ann_date,end_date,holder_name,hold_amount,hold_ratio"


def fetch_top10_holder(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.1,
    **options: Any,
) -> pd.DataFrame:
    """Fetch top-10 float-holder rows for all listed instruments and quarters."""
    stocks = _get_stock_list(client, options.get("max_retries", 3))
    if not stocks:
        logger.warning("[top10_holder] 股票列表为空，无法下载")
        return pd.DataFrame()

    quarters = quarter_end_dates(start, end)
    logger.info("[top10_holder] 股票 %d 只, 报告期 %d 个", len(stocks), len(quarters))
    max_retries = options.get("max_retries", 3)

    all_rows: list[pd.DataFrame] = []
    count = 0
    for ts_code in stocks:
        for q_end in quarters:
            count += 1
            if count % 100 == 0:
                logger.info("[top10_holder] 进度: %d", count)
            df = retry_call(
                client.top10_floatholders,
                ts_code=ts_code,
                end_date=q_end,
                fields=KEY_FIELDS,
                logger=logger,
                label=f"[top10_holder] {ts_code}@{q_end}",
                max_retries=max_retries,
            )
            if df is not None and not df.empty:
                all_rows.append(df)
            if sleep > 0:
                time.sleep(sleep)

    if not all_rows:
        logger.warning("[top10_holder] 全期无数据")
        return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)


def _get_stock_list(client: Any, max_retries: int) -> list[str]:
    logger.info("[top10_holder] 获取股票列表...")
    df = retry_call(
        client.stock_basic,
        list_status="L",
        fields="ts_code",
        logger=logger,
        label="[top10_holder] stock_basic",
        max_retries=max_retries,
    )
    if df is None or df.empty:
        return []
    stocks = df["ts_code"].tolist()
    logger.info("[top10_holder] 获取到 %d 只股票", len(stocks))
    return stocks
