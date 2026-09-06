"""Northbound per-stock net-buy fetcher (legacy scope ``trade_data/northbound``).

Non-Tushare scope: the legacy ``northbound_netbuy.parquet`` comes from
Eastmoney HSGT holdings via akshare (``stock_hsgt_individual_em``), walked per
stock.  The Chinese column names are normalized to the legacy on-disk columns
(``ts_code``/``trade_date``/``net_buy``/``net_buy_shares``/``hold_shares``/
``hold_market_cap``/``close``/``pct_chg``) — that normalization reproduces the
legacy layout this scope defined, values stay raw.  Rows are filtered to the
requested range and landed at ``{year}/northbound_netbuy.parquet``.

The akshare module (or any object providing ``stock_hsgt_individual_em``) is
injectable via the ``akshare`` option; without it the fetcher falls back to
``import akshare``.  Candidate codes come from the ``codes`` option, else from
``stock_info_a_code_name`` on the same module.  Northbound disclosure stopped
2024-08-17, so the source is naturally bounded.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from data.downloader.base import retry_call

logger = logging.getLogger(__name__)

# akshare 中文列 -> 旧卷布局列
COLUMN_MAP = {
    "持股日期": "trade_date",
    "今日增持股数": "net_buy_shares",
    "今日增持资金": "net_buy",
    "持股数量": "hold_shares",
    "持股市值": "hold_market_cap",
    "当日收盘价": "close",
    "当日涨跌幅": "pct_chg",
}

OUTPUT_COLUMNS = [
    "trade_date", "net_buy_shares", "net_buy", "hold_shares",
    "hold_market_cap", "close", "pct_chg", "ts_code",
]


def fetch_northbound(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.15,
    **options: Any,
) -> pd.DataFrame:
    """Fetch per-stock northbound net-buy rows for the inclusive range."""
    ak = options.get("akshare")
    if ak is None:
        try:
            import akshare as ak  # type: ignore[no-redef]
        except ImportError:
            logger.error("[northbound] akshare 未安装且未注入 akshare 选项")
            return pd.DataFrame()

    max_retries = options.get("max_retries", 2)
    codes = options.get("codes")
    if not codes:
        try:
            info = ak.stock_info_a_code_name()
            codes = info["code"].tolist()
        except Exception as e:
            logger.error("[northbound] 获取股票列表失败: %s", e)
            return pd.DataFrame()

    all_rows: list[pd.DataFrame] = []
    for i, code in enumerate(codes, 1):
        pure_code = code.split(".")[0]
        ts_code = _code_to_ts(pure_code)
        df = retry_call(
            ak.stock_hsgt_individual_em,
            symbol=pure_code,
            logger=logger,
            label=f"[northbound] {ts_code}",
            max_retries=max_retries,
        )
        if df is None or df.empty:
            continue

        frame = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
        if "trade_date" not in frame.columns:
            continue
        frame["trade_date"] = frame["trade_date"].astype(str).str.replace("-", "", regex=False)
        window = frame[(frame["trade_date"] >= start) & (frame["trade_date"] <= end)]
        if window.empty:
            continue
        window = window.copy()
        window["ts_code"] = ts_code
        all_rows.append(window)
        if i % 50 == 0:
            logger.info("[northbound] 进度: %d / %d", i, len(codes))
        if sleep > 0:
            time.sleep(sleep)

    if not all_rows:
        logger.warning("[northbound] 无数据")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    for col in OUTPUT_COLUMNS:
        if col not in result.columns:
            result[col] = pd.NA
    return result[OUTPUT_COLUMNS]


def _code_to_ts(code: str) -> str:
    """纯数字代码转 tushare 格式（000001 -> 000001.SZ）。"""
    if "." in code:
        return code
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"
