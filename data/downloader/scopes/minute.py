"""Intraday (stk_mins) bars fetcher (legacy scope ``trade_data/minute``).

Calls the vendor ``stk_mins`` interface per instrument code and lands the raw
rows at ``{year}/minute_{freq}.parquet`` (e.g. ``2024/minute_30min.parquet``).
This layout has no MigrationMapping template (the release catalog serves
minute bars from the per-day tick volume), so the scope registers a custom
writer instead of the mapping-driven one.  Vendor fields stay raw.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

from data.downloader.base import RawVolumeWriter, retry_call

logger = logging.getLogger(__name__)


def fetch_minute(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.3,
    **options: Any,
) -> pd.DataFrame:
    """Fetch intraday bars for ``codes`` (required option) at ``freq``."""
    codes = options.get("codes")
    freq = options.get("freq", "30min")
    max_retries = options.get("max_retries", 3)
    if not codes:
        logger.warning("[minute] 未指定股票代码，请提供 codes 选项")
        return pd.DataFrame()

    start_fmt = f"{start[:4]}-{start[4:6]}-{start[6:8]} 09:00:00"
    end_fmt = f"{end[:4]}-{end[4:6]}-{end[6:8]} 15:00:00"

    all_rows: list[pd.DataFrame] = []
    for code in codes:
        logger.info("[minute] 下载 %s %s ...", code, freq)
        df = retry_call(
            client.stk_mins,
            ts_code=code,
            freq=freq,
            start_date=start_fmt,
            end_date=end_fmt,
            logger=logger,
            label=f"[minute] {code}",
            max_retries=max_retries,
        )
        if df is None or df.empty:
            logger.warning("[minute] %s 无数据", code)
        else:
            all_rows.append(df)
            logger.info("[minute] %s 获取 %d 行", code, len(df))
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[minute] 无数据")
        return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)


def write_minute(
    writer: RawVolumeWriter,
    frame: pd.DataFrame,
    *,
    freq: str = "30min",
    **options: Any,
) -> list[Path]:
    """Custom writer: ``{root}/{year}/minute_{freq}.parquet`` grouped by ``trade_time``."""
    if frame.empty:
        return []
    filename = f"minute_{freq}.parquet"
    if "trade_time" not in frame.columns:
        raise ValueError(f"minute rows need a trade_time column, got {list(frame.columns)}")
    digits = frame["trade_time"].astype(str).str.replace("-", "", regex=False).str[:8].str[:4]
    written: list[Path] = []
    for year, group in frame.groupby(digits, sort=True):
        target = writer.root / str(year) / filename
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
    keys = ["ts_code", "trade_time"]
    if all(k in merged.columns for k in keys):
        merged = merged.drop_duplicates(subset=keys, keep="last")
    return merged
