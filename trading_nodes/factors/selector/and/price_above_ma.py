"""股价站上均线选股因子。

声明式选股因子：请求近 N 日收盘价历史，当股票最新收盘价站上其 N 日均线
时标记为通过(1)，用于"条件过滤"类选股（只保留价格在均线上方的标的）。
"""

from __future__ import annotations

from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from trading_nodes_base.factors import BinarySelectionFactor


class PriceAboveMaSelectionFactor(BinarySelectionFactor):
    """二值选股因子：收盘价 > N 日均线 -> 选中(1)，否则剔除(0)。"""

    def __init__(self, window: int = 20) -> None:
        super().__init__("price-above-ma")
        self.window = window
        self._values: dict[str, list[float]] = {}  # 每只股票近 window 根收盘价

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[dict]:
        # 声明式查询：按日请求候选池的收盘价行情。
        self._data_clear()
        return [{"type": "market/daily", "date": date, "codes": codes or [], "period": ["close"]}]

    def _calculate_internal(self, data_cache: dict) -> pd.Series:
        values: dict[str, float] = {}
        for payload in data_cache.values():
            records = getattr(payload, "records", None)
            if records is not None:
                payload = records
            rows = payload.to_dict("records") if hasattr(payload, "to_dict") else payload
            for row in rows or []:
                if isinstance(row, dict):
                    code = row.get("code") or row.get("ts_code")
                    close = row.get("close")
                else:
                    code = getattr(row, "instrument_id", None) or getattr(row, "code", None)
                    close = getattr(row, "close", None)
                if code and isinstance(close, (int, float)):
                    history = self._values.setdefault(code, [])
                    history.append(float(close))
                    history[:] = history[-self.window:]  # 只保留近 window 根
                    # 收盘价高于均线(且历史足够) -> 1，否则 0
                    values[code] = float(len(history) >= self.window and close > sum(history) / len(history))
        return pd.Series(values, dtype=float)


__all__ = ["PriceAboveMaSelectionFactor"]
