"""Stock master fetcher (legacy scope ``stock/basic``).

Calls the vendor ``stock_basic`` interface for all listed A-share instruments
and lands the raw frame at ``{root}/stock_basic.parquet`` (global snapshot,
deduplicated on ``ts_code``).  Vendor fields stay untouched: the
``list_status`` L/D/P decoding happens in the adapters.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from data.downloader.base import retry_call

logger = logging.getLogger(__name__)

KEY_FIELDS = (
    "ts_code,symbol,name,area,industry,fullname,market,"
    "exchange,list_status,list_date,delist_date,is_hs"
)


def fetch_stock_basic(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.0,
    **options: Any,
) -> pd.DataFrame:
    """Fetch the full listed-instrument master (start/end unused, snapshot scope)."""
    list_status = options.get("list_status", "L")
    df = retry_call(
        client.stock_basic,
        exchange=options.get("exchange", ""),
        list_status=list_status,
        fields=KEY_FIELDS,
        logger=logger,
        label="[stock_basic]",
        max_retries=options.get("max_retries", 3),
    )
    if df is None or df.empty:
        logger.warning("[stock_basic] 无数据 (list_status=%s)", list_status)
        return pd.DataFrame()
    logger.info("[stock_basic] 拉到 %d 行", len(df))
    return df
