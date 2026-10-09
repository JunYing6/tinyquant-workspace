"""分钟线（stk_mins）获取器（旧作用域 ``trade_data/minute``）。

按每个标的代码调用厂商 ``stk_mins`` 接口，将原始行写入 per-day
``{year}/{MMDD}/kline.parquet``（统一 kline schema，``timeframe`` 列标记频率，
如 ``30m``；按 ``ts_code``+``trade_date``+``trade_time``+``data_type``+``timeframe`` 去重）。
厂商字段保持原样。
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
    """按 ``freq`` 拉取 ``codes``（必填选项）的分钟线。"""
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
    """写入器：分钟线统一落入按日 ``{root}/{year}/{MMDD}/kline.parquet``。

    与日线共用一份 ``kline`` schema，用 ``timeframe`` 频率列区分（如 ``30m``）：
    分钟行填 ``trade_date``（当日）+ ``trade_time``（完整时间）+ ``timeframe``；
    其余日线专属列留空。按 ``ts_code``+``trade_date``+``trade_time``+``data_type``+``timeframe`` 去重合并。
    """
    if frame.empty:
        return []
    if "trade_time" not in frame.columns:
        raise ValueError(f"minute rows need a trade_time column, got {list(frame.columns)}")

    from data.downloader.scopes.daily import KLINE_COLUMNS

    data_type = str(options.get("data_type", "stock"))
    frame = frame.copy()
    ts = pd.to_datetime(frame["trade_time"])
    frame["trade_time"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    frame["trade_date"] = ts.dt.strftime("%Y%m%d")
    frame["data_type"] = data_type
    frame["timeframe"] = _freq_label(freq)
    for col in KLINE_COLUMNS:
        if col not in frame.columns:
            frame[col] = pd.NA
    frame = frame[KLINE_COLUMNS]

    written: list[Path] = []
    for day, group in frame.groupby("trade_date", sort=True):
        day = str(day)
        path = Path(writer.root) / day[:4] / day[4:8] / "kline.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_file():
            existing = pd.read_parquet(path)
            group = pd.concat([existing, group], ignore_index=True)
        group = group.drop_duplicates(subset=KLINE_DEDUP_KEYS, keep="last")
        group.to_parquet(path, index=False)
        written.append(path)
    return written


KLINE_DEDUP_KEYS = ["ts_code", "trade_date", "trade_time", "data_type", "timeframe"]


def _freq_label(freq: str) -> str:
    return str(freq).strip().lower().replace("min", "m")
