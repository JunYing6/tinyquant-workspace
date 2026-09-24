"""动量选股因子。

声明式选股因子：请求近 N 日收盘价历史，计算区间动量收益率（最新收盘价 / 
N 日前收盘价 - 1），作为"数值打分"类选股因子，用于对候选股票进行排序。
"""

from __future__ import annotations

from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from trading_nodes_base.factors import FloatSelectionFactor


class MomentumSelectionFactor(FloatSelectionFactor):
    """浮点选股因子：输出每只股票过去 window 个交易日的动量收益率。"""

    def __init__(self, window: int = 20) -> None:
        super().__init__("momentum-selection")
        self.window = window
        self._values: dict[str, list[float]] = {}  # 每只股票近 window+1 根收盘价

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
                    history[:] = history[-self.window - 1:]  # 只保留近 window+1 根
                    if len(history) > self.window:
                        # 区间动量：最新价 / window 根之前的最初价 - 1
                        values[code] = history[-1] / history[-self.window - 1] - 1.0
        return pd.Series(values, dtype=float)


__all__ = ["MomentumSelectionFactor"]
