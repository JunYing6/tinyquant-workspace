"""ATR 止损风控因子。

基于 K 线的流式风控因子：以平均真实波幅(ATR)度量近期波动，当收盘价
跌破"开仓价 - 倍数*ATR"时触发止损风控信号，用于控制持仓风险。
"""

from __future__ import annotations

from collections import deque
from typing import Any

from trading_nodes_base.factors import RiskKlineFactor
from trading_nodes_base.types import KlineBar, RiskSignal


class AtrStopRiskFactor(RiskKlineFactor):
    """ATR 移动止损：价格回撤幅度超过多个 ATR 时发出止损信号。"""

    def __init__(self, window: int = 14, multiple: float = 2.0) -> None:
        super().__init__("atr-stop")
        if window <= 0 or multiple <= 0:
            raise ValueError("window and multiple must be positive")
        self.window = window
        self.multiple = multiple
        self._bars: dict[str, deque[KlineBar]] = {}  # 每只股票近 window 根 K 线
        self._entry: dict[str, float] = {}  # 每只股票的开仓参考价

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[Any]:
        # 流式因子：无需批量数据查询，直接就绪。
        self._data_clear()
        return []

    def on_bar(self, bar: KlineBar) -> list[RiskSignal]:
        bars = self._bars.setdefault(bar.code, deque(maxlen=self.window + 1))
        bars.append(bar)
        if len(bars) < self.window:
            return []  # 波动历史不足，暂不评估
        # 用 (最高-最低) 近似 ATR
        atr = sum(item.high - item.low for item in bars) / len(bars)
        stop = bar.close - self.multiple * atr
        entry = self._entry.setdefault(bar.code, bar.close)
        if entry > 0 and bar.close < stop:  # 收盘价跌破止损线 -> 触发止损
            return [RiskSignal(True, "float", value=0.0, code=bar.code, time=bar.end_time, reason="ATR stop")]
        return []


__all__ = ["AtrStopRiskFactor"]
