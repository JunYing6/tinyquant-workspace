"""Consensus profit-forecast fetcher (legacy scope ``fina/consensus``).

Non-Tushare scope: the legacy workspace sourced THS consensus forecasts via
akshare (``stock_profit_forecast_ths``), walked per stock, and wrote yearly
``{year}/consensus_forecast.parquet`` files (``ts_code``/``year``/
``eps_mean``/``np_mean``).  The release mapping marks ``fina/consensus``
contract-only (no physical source on the workspace volume — the adapters
reject requests), so this scope registers a custom writer for the legacy
yearly layout and exists to keep the download capability.

The akshare module is injectable via the ``akshare`` option (any object with
``stock_profit_forecast_ths``); without it the fetcher falls back to
``import akshare``.  Candidate codes come from the ``codes`` option, else from
``stock_info_a_code_name`` on the same module.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

from data.downloader.base import RawVolumeWriter

logger = logging.getLogger(__name__)


def fetch_consensus(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.3,
    **options: Any,
) -> pd.DataFrame:
    """Fetch per-stock consensus EPS/net-profit forecasts for future years."""
    ak = options.get("akshare")
    if ak is None:
        try:
            import akshare as ak  # type: ignore[no-redef]
        except ImportError:
            logger.error("[consensus] akshare 未安装且未注入 akshare 选项")
            return pd.DataFrame()

    codes = options.get("codes")
    if not codes:
        try:
            info = ak.stock_info_a_code_name()
            codes = info["code"].tolist()
        except Exception as e:
            logger.error("[consensus] 获取股票列表失败: %s", e)
            return pd.DataFrame()

    all_rows: list[dict[str, Any]] = []
    failed = 0
    for code in codes:
        pure_code = code.split(".")[0]
        ts_code = _code_to_ts(pure_code)
        try:
            eps_df = ak.stock_profit_forecast_ths(symbol=pure_code, indicator="预测年报每股收益")
            np_df = ak.stock_profit_forecast_ths(symbol=pure_code, indicator="预测年报净利润")
        except Exception as e:
            failed += 1
            if failed <= 10:
                logger.debug("[consensus] %s 失败: %s", code, e)
            if sleep > 0:
                time.sleep(sleep)
            continue

        eps_by_year = _parse_forecast(eps_df)
        np_by_year = _parse_forecast(np_df)
        for year in set(eps_by_year) | set(np_by_year):
            all_rows.append(
                {
                    "ts_code": ts_code,
                    "year": year,
                    "eps_mean": eps_by_year.get(year),
                    "np_mean": np_by_year.get(year),
                }
            )
        if sleep > 0:
            time.sleep(sleep)

    logger.info("[consensus] 完成: 候选 %d, 失败 %d, 行数 %d", len(codes), failed, len(all_rows))
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


def _parse_forecast(df: pd.DataFrame | None) -> dict[int, float]:
    """Extract {year: mean-forecast} from a THS forecast frame (vendor layout)."""
    if df is None or df.empty:
        return {}
    by_year: dict[int, float] = {}
    for _, row in df.iterrows():
        try:
            year = int(row.iloc[0])
            by_year[year] = float(row.iloc[2])  # 均值
        except (ValueError, TypeError):
            continue
    return by_year


def write_consensus(
    writer: RawVolumeWriter,
    frame: pd.DataFrame,
    **options: Any,
) -> list[Path]:
    """Custom writer: one ``{root}/{year}/consensus_forecast.parquet`` per year."""
    if frame.empty:
        return []
    written: list[Path] = []
    for year, group in frame.groupby("year"):
        target = writer.root / str(int(year)) / "consensus_forecast.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        merged = _merge(target, group)
        merged.to_parquet(target, index=False)
        written.append(target)
    return written


def _merge(target: Path, payload: pd.DataFrame) -> pd.DataFrame:
    if not target.is_file():
        return payload
    existing = pd.read_parquet(target)
    merged = pd.concat([existing, payload], ignore_index=True)
    return merged.drop_duplicates(subset=["ts_code"], keep="last")


def _code_to_ts(code: str) -> str:
    """纯数字代码转 tushare 格式（000001 -> 000001.SZ）。"""
    if "." in code:
        return code
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"
