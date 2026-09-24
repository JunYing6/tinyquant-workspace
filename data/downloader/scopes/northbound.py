"""北向个股净买入获取器（旧作用域 ``trade_data/northbound``）。

非 Tushare 作用域：旧版 ``northbound_netbuy.parquet`` 来自东方财富沪深港通持股，通过 akshare
（``stock_hsgt_individual_em``）按个股抓取。中文列名被规范化为旧版磁盘列
（``ts_code``/``trade_date``/``net_buy``/``net_buy_shares``/``hold_shares``/
``hold_market_cap``/``close``/``pct_chg``）——该规范化复现的是本作用域定义的旧布局，
值保持原样。行被过滤到请求的区间，并写入 ``{year}/northbound_netbuy.parquet``。

akshare 模块（或任何提供 ``stock_hsgt_individual_em`` 的对象）可通过 ``akshare`` 选项注入；
未注入时获取器回退到 ``import akshare``。候选代码来自 ``codes`` 选项，否则取自同一模块的
``stock_info_a_code_name``。北向披露已于 2024-08-17 停止，因此该数据源有天然上界。
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
    """按闭区间拉取个股北向净买入行。"""
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
