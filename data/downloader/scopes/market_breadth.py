"""市场宽度聚合器（旧作用域 ``trade_data/breadth``）。

派生作用域，之所以放在下载器中只是因为旧卷就是这样存储的：它从年度库
``{year}/year.duckdb#kline`` 读取已下载好的日线（``timeframe='1d'``），按日把
上涨/下跌/总成交金额聚合成 ``{year}/year.duckdb`` 表的 ``market_breadth``。
此处没有厂商调用；金额是 K 线 ``amount`` 列按其厂商单位的求和（不做换算）。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from data.downloader.base import read_duckdb_table

logger = logging.getLogger(__name__)


def fetch_market_breadth(
    client: Any,
    start: str,
    end: str,
    *,
    data_root: str | Path | None = None,
    **options: Any,
) -> pd.DataFrame:
    """从原始日线 K 线文件聚合市场宽度（不使用客户端）。"""
    if data_root is None:
        raise ValueError(
            "trade_data/breadth reads the already-downloaded bar volume; "
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
    db_path = root / str(year) / "year.duckdb"
    if not db_path.is_file():
        logger.warning("[breadth] %s 无年度库，跳过 %d", db_path, year)
        return pd.DataFrame()

    try:
        df = read_duckdb_table(db_path, "kline")
    except Exception as e:
        logger.error("[breadth] 读取 %s 失败: %s", year, e)
        return pd.DataFrame()

    if "timeframe" in df.columns:
        df = df[df["timeframe"] == "1d"]
    if df.empty or "pct_chg" not in df.columns or "amount" not in df.columns:
        logger.warning("[breadth] %d 日线字段缺失，跳过", year)
        return pd.DataFrame()

    df["pct_chg"] = df["pct_chg"].fillna(0)
    grouped = df.groupby("trade_date").agg(
        up_amount=("amount", lambda s: s[df.loc[s.index, "pct_chg"] > 0].sum()),
        down_amount=("amount", lambda s: s[df.loc[s.index, "pct_chg"] < 0].sum()),
        total_amount=("amount", "sum"),
    ).reset_index()
    grouped["trade_date"] = grouped["trade_date"].astype(str)
    return grouped
