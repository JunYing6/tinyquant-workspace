"""Index membership (weights) fetcher (legacy scope ``index/member``).

Calls the vendor ``index_weight`` interface per index code and lands each
trading day's constituent rows at ``{year}/{MMDD}/index_member.parquet``
(``index_code``/``con_code``/``trade_date``/``weight``).  The
``index.member`` mapping declares this per-day layout as a description rather
than a path template, so the scope registers a custom writer that reproduces
the legacy per-day directory layout exactly.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

from data.downloader.base import RawVolumeWriter, retry_call

logger = logging.getLogger(__name__)

DEFAULT_INDEXES = [
    "000300.SH",  # 沪深300
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "399101.SZ",  # 深证100指数
    "399102.SZ",  # 中小板100指数
]


def fetch_index_member(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.3,
    **options: Any,
) -> pd.DataFrame:
    """Fetch constituent weights for the configured index codes."""
    index_codes = options.get("index_codes") or DEFAULT_INDEXES
    max_retries = options.get("max_retries", 3)

    all_rows: list[pd.DataFrame] = []
    for index_code in index_codes:
        logger.info("[index_member] 下载 %s %s~%s ...", index_code, start, end)
        df = retry_call(
            client.index_weight,
            index_code=index_code,
            start_date=start,
            end_date=end,
            logger=logger,
            label=f"[index_member] {index_code}",
            max_retries=max_retries,
        )
        if df is not None and not df.empty:
            all_rows.append(df)
            logger.info("[index_member] %s 获取 %d 行", index_code, len(df))
        else:
            logger.warning("[index_member] %s 无数据", index_code)
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[index_member] 无数据")
        return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)


def write_index_member(
    writer: RawVolumeWriter,
    frame: pd.DataFrame,
    **options: Any,
) -> list[Path]:
    """Custom writer: one ``{root}/{year}/{MMDD}/index_member.parquet`` per day."""
    if frame.empty:
        return []
    if "trade_date" not in frame.columns:
        raise ValueError(f"index_member rows need trade_date, got {list(frame.columns)}")

    written: list[Path] = []
    for trade_date, group in frame.groupby("trade_date"):
        date_str = str(trade_date)
        target = writer.root / date_str[:4] / date_str[4:8] / "index_member.parquet"
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
    keys = ["index_code", "con_code"]
    if all(k in merged.columns for k in keys):
        merged = merged.drop_duplicates(subset=keys, keep="last")
    return merged
