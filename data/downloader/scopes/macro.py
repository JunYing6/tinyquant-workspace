"""宏观指标获取器（旧作用域 ``macro/indicator``）。

从厂商拉取月度 CPI/PMI（``cn_cpi``/``cn_pmi``），并从 akshare（可选）拉取 EPU 指数，
堆叠为旧版长表布局 ``{root}/macro.parquet``（``month``/``indicator``/``value``，
``indicator`` 取 cpi/pmi/epu）。厂商值保持原样（CPI 值为厂商的同比数值）。EPU 来源可通过
``epu_fetcher`` 选项注入；未注入时本模块回退到 ``import akshare``，并在 akshare 不可用时跳过该序列。
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
    """将 CPI/PMI(/EPU) 月度序列拉取为长格式 macro 布局。"""
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
    """从注入的获取器或 akshare 拉取 EPU 月度指数。"""
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
