"""一致预期（盈利预测）获取器（旧作用域 ``fina/consensus``）。

非 Tushare 作用域：旧工作区通过 akshare（``stock_profit_forecast_ths``）按个股获取同花顺
一致预期，并逐年存储行（``ts_code``/``year``/``eps_mean``/``np_mean``）到年度 duckdb 表
``consensus_forecast``。发布映射把 ``fina/consensus`` 标记为仅契约（适配层无实际数据来源，
请求会失败），因此本作用域为年度表注册了自定义写入器，其存在是为了保留下载能力。

akshare 模块可通过 ``akshare`` 选项注入（任何带 ``stock_profit_forecast_ths`` 的对象）；
未注入时获取器回退到 ``import akshare``。候选代码来自 ``codes`` 选项，否则取自同一模块的
``stock_info_a_code_name``。
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

from data.downloader.base import RawVolumeWriter, merge_duckdb_table

logger = logging.getLogger(__name__)


def fetch_consensus(
    client: Any,
    start: str,
    end: str,
    *,
    sleep: float = 0.3,
    **options: Any,
) -> pd.DataFrame:
    """按个股拉取未来年度的一致预期 EPS / 净利润预测。"""
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
    """从同花顺预测表（厂商布局）中提取 {year: 平均预测}。"""
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
    """自定义写入器：``{root}/{year}/year.duckdb`` 表的 ``consensus_forecast``。"""
    if frame.empty:
        return []
    frame = frame.copy()
    frame["year"] = frame["year"].astype(int)
    written: list[Path] = []
    for year, group in frame.groupby("year"):
        db_path = writer.root / str(int(year)) / "year.duckdb"
        merge_duckdb_table(db_path, "consensus_forecast", group, ["ts_code"])
        written.append(db_path)
    return written


def _code_to_ts(code: str) -> str:
    """纯数字代码转 tushare 格式（000001 -> 000001.SZ）。"""
    if "." in code:
        return code
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"
