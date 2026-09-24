"""十大流通股东获取器（旧作用域 ``event/top10_holder``）。

遍历每个已上市标的（来自厂商 ``stock_basic`` 接口）× 每个报告季度，按配对调用
``top10_floatholders``，将原始行写入 ``{root}/top10_holder.parquet``（全局表，按
``ts_code``+``end_date``+``holder_name`` 去重）。旧版的临时文件/断点续传机制折叠进写入器的
合并去重：重跑某区间会刷新已有行而非重复插入。
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
    """拉取所有已上市标的各季度的十大流通股东行。"""
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
