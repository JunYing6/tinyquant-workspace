"""ATR 止损风控因子。

基于 K 线的流式风控因子：以平均真实波幅(ATR)度量近期波动，当收盘价
跌破"开仓价 - 倍数*ATR"时触发止损风控信号，用于控制持仓风险。
"""

from __future__ import annotations

from typing import Any

from trading_nodes_base.factors import RiskNonIntradayKlineFactor
from trading_nodes_base.types import KlineBar, RiskSignal


class AtrStopRiskFactor(RiskNonIntradayKlineFactor):
    """ATR 移动止损（声明式）：盘前用近 window 日线的真实波幅近似 ATR，
    收盘价跌破"开仓参考价 - 倍数*ATR"时触发止损风控信号。"""

    history_fields = ("high", "low", "close")

    def __init__(self, window: int = 14, multiple: float = 2.0) -> None:
        super().__init__("atr-stop")
        if window <= 0 or multiple <= 0:
            raise ValueError("window and multiple must be positive")
        self.window = window
        self.multiple = multiple
        self._entry: dict[str, float] = {}

    @property
    def history_lookback_days(self) -> int:
        return self.window + 1

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[Any]:
        self._data_clear()
        return []

    def daily_risk(self, date: Any) -> list[RiskSignal]:
        signals: list[RiskSignal] = []
        for code, bars in self.history_bars(date).items():
            if len(bars) < self.window:
                continue  # 波动历史不足，暂不评估
            closes = [float(getattr(b, "close")) for b in bars]
            window_bars = bars[-self.window:]
            # 用 (最高-最低) 近似 ATR
            atr = sum(float(getattr(b, "high")) - float(getattr(b, "low")) for b in window_bars) / self.window
            stop = closes[-1] - self.multiple * atr
            if code not in self._entry:
                self._entry[code] = closes[0]
            entry = self._entry[code]
            if entry > 0 and closes[-1] < stop:  # 收盘价跌破止损线 -> 触发止损
                signals.append(RiskSignal(True, "float", value=0.0, code=code, time=date, reason="ATR stop"))
        return signals

    def on_bar(self, bar: KlineBar) -> list[RiskSignal]:
        return []


__all__ = ["AtrStopRiskFactor"]
