"""Macro indicator fetcher (legacy scope ``macro/indicator``).

Fetches monthly CPI/PMI from the vendor (``cn_cpi``/``cn_pmi``) and the EPU
index from akshare (optional), stacking them into the long legacy layout
``{root}/macro.parquet`` (``month``/``indicator``/``value``, ``indicator`` in
cpi/pmi/epu).  Vendor values stay raw (the CPI value is the vendor's year-on-
year figure).  The EPU source is injectable via the ``epu_fetcher`` option;
without it the module falls back to ``import akshare`` and skips the series
when akshare is unavailable.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)


def fetch_macro(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.0,
    **options: Any,
) -> pd.DataFrame:
    """Fetch CPI/PMI(/EPU) monthly series into the long macro layout."""
    start_month = start[:6]
    end_month = end[:6]

    epu_fetcher = options.get("epu_fetcher")
    frames = [
        _download_cpi(client, start_month, end_month),
        _download_pmi(client, start_month, end_month),
        _download_epu(epu_fetcher),
    ]
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        logger.warning("[macro] 全指标无数据")
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True)
    return merged[["month", "indicator", "value"]].sort_values(
        ["indicator", "month"]
    ).reset_index(drop=True)


def _download_cpi(client: Any, start_month: str, end_month: str) -> pd.DataFrame | None:
    try:
        df = client.cn_cpi(start_month=start_month, end_month=end_month)
    except Exception as e:
        logger.error("[cpi] 请求失败: %s", e)
        return None
    if df is None or df.empty:
        logger.warning("[cpi] 无数据")
        return None
    return pd.DataFrame(
        {
            "month": df["month"].astype(str),
            "indicator": "cpi",
            "value": df["nt_yoy"].astype(float),
        }
    )


def _download_pmi(client: Any, start_month: str, end_month: str) -> pd.DataFrame | None:
    try:
        df = client.cn_pmi(start_month=start_month, end_month=end_month)
    except Exception as e:
        logger.error("[pmi] 请求失败: %s", e)
        return None
    if df is None or df.empty:
        logger.warning("[pmi] 无数据")
        return None
    return pd.DataFrame(
        {
            "month": df["MONTH"].astype(str),
            "indicator": "pmi",
            "value": df["PMI010000"].astype(float),
        }
    )


def _download_epu(epu_fetcher: Callable[[], pd.DataFrame] | None) -> pd.DataFrame | None:
    """Fetch the EPU monthly index from an injected fetcher or akshare."""
    if epu_fetcher is None:
        try:
            import akshare as ak

            epu_fetcher = ak.article_epu_index
        except ImportError:
            logger.warning("[epu] akshare 未安装，跳过")
            return None
    try:
        df = epu_fetcher()
    except Exception as e:
        logger.error("[epu] 请求失败: %s", e)
        return None
    if df is None or df.empty:
        logger.warning("[epu] 无数据")
        return None

    month_col = (
        df["year"].astype(int).astype(str)
        + df["month"].astype(int).astype(str).str.zfill(2)
    )
    value_col = "China_Policy_Index" if "China_Policy_Index" in df.columns else df.columns[-1]
    return pd.DataFrame(
        {
            "month": month_col,
            "indicator": "epu",
            "value": df[value_col].astype(float),
        }
    )
