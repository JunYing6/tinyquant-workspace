"""SW industry membership fetcher (legacy scope ``sw/industry``).

Walks the 28 SW level-1 industry indexes and calls the vendor ``index_member``
interface per industry, landing the raw membership rows at
``{root}/sw_industry.parquet`` (``index_code``/``con_code``/``in_date``/
``out_date``/``is_new`` — exactly the columns the industry-membership adapter
aliases).  Membership validity decoding happens in the adapters.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from data.downloader.base import retry_call

logger = logging.getLogger(__name__)

# 申万一级行业代码（28个）
DEFAULT_SW_CODES = [
    "801010", "801020", "801030", "801040", "801050", "801080",
    "801110", "801120", "801130", "801140", "801150", "801160",
    "801170", "801180", "801200", "801210", "801230", "801710",
    "801720", "801730", "801740", "801750", "801760", "801770",
    "801780", "801790", "801880", "801890",
]


def fetch_sw_industry(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.3,
    **options: Any,
) -> pd.DataFrame:
    """Fetch SW L1 membership rows for every industry code (start/end unused)."""
    sw_codes = options.get("sw_codes") or DEFAULT_SW_CODES

    all_dfs: list[pd.DataFrame] = []
    for ind_code in sw_codes:
        index_code = f"{ind_code}.SI"
        logger.info("[sw_industry] 下载 %s ...", index_code)
        df = retry_call(
            client.index_member,
            index_code=index_code,
            logger=logger,
            label=f"[sw_industry] {index_code}",
            max_retries=options.get("max_retries", 3),
        )
        if df is not None and not df.empty:
            all_dfs.append(df)
            logger.info("[sw_industry] %s 获取 %d 行", ind_code, len(df))
        else:
            logger.warning("[sw_industry] %s 无数据", index_code)
        if sleep > 0:
            time.sleep(sleep)

    if not all_dfs:
        logger.warning("[sw_industry] 无数据可保存")
        return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)
