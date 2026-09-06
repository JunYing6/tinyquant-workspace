"""Market breadth aggregator (legacy scope ``trade_data/breadth``).

Derived scope, kept in the downloader only because the legacy volume stores it
that way: it reads the already-downloaded ``{year}/kline.parquet`` from the raw
volume and aggregates per-day up/down/total traded amount into
``{year}/market_breadth.parquet``.  No vendor call happens here; the amounts
are sums of the kline ``amount`` column in its vendor unit (no conversion).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def fetch_market_breadth(
    client: Any,
    start: str,
    end: str,
    *,
    data_root: str | Path | None = None,
    **options: Any,
) -> pd.DataFrame:
    """Aggregate market breadth from the raw kline volume (client unused)."""
    if data_root is None:
        raise ValueError(
            "trade_data/breadth reads the already-downloaded kline volume; "
            "a data_root (RawVolumeWriter root) is required"
        )
    root = Path(data_root)

    frames: list[pd.DataFrame] = []
    for year in range(int(start[:4]), int(end[:4]) + 1):
        df = _aggregate_year(root, year)
        if not df.empty:
            frames.append(df)

    if not frames:
        logger.warning("[breadth] 无数据")
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _aggregate_year(root: Path, year: int) -> pd.DataFrame:
    path = root / str(year) / "kline.parquet"
    if not path.is_file():
        logger.warning("[breadth] %s 不存在，跳过 %d", path, year)
        return pd.DataFrame()

    try:
        df = pd.read_parquet(path)
    except Exception as e:
        logger.error("[breadth] 读取 %s 失败: %s", path, e)
        return pd.DataFrame()

    if df.empty or "pct_chg" not in df.columns or "amount" not in df.columns:
        logger.warning("[breadth] %s 字段缺失，跳过", path)
        return pd.DataFrame()

    df["pct_chg"] = df["pct_chg"].fillna(0)
    grouped = df.groupby("trade_date").agg(
        up_amount=("amount", lambda s: s[df.loc[s.index, "pct_chg"] > 0].sum()),
        down_amount=("amount", lambda s: s[df.loc[s.index, "pct_chg"] < 0].sum()),
        total_amount=("amount", "sum"),
    ).reset_index()
    grouped["trade_date"] = grouped["trade_date"].astype(str)
    return grouped
