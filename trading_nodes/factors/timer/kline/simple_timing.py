"""日线 K 线择时因子集合。

这些因子均为"流式因子"：不声明批量数据查询（get_query_lst 返回空），
而是由引擎逐根推送日 K 线(KlineBar)，在 on_bar() 中实时计算买卖信号
(SignalIntent)，用于决定何时进/出股票。基类 _DailyTimingFactor 统一了
"无需查询、直接就绪"的声明约定。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from trading_nodes_base.factors import NonIntradayKlineFactor
from trading_nodes_base.types import KlineBar, SignalIntent


class _DailyTimingFactor(NonIntradayKlineFactor):
    """择时因子公共基类：声明无需数据查询，并提供统一的信号构造助手。"""

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[Any]:
        # 流式因子不走声明式取数：重置代次并标记就绪，返回空请求列表。
        self._data_clear()
        return []

    def _intent(self, bar: KlineBar, action: str, reason: str, sizing: float | None = None) -> SignalIntent:
        # 只发裸信号（默认不带数量），仓位由择时器按目标池等权计算；
        # 显式传 sizing 则保留（透传，不被择时器覆盖）。
        metadata = {"sizing_intent": sizing} if sizing is not None else {}
        return SignalIntent(bar.code, action, bar.end_time, reason, metadata)

    def _intent_at(self, code: str, when: Any, action: str, reason: str) -> SignalIntent:
        return SignalIntent(code, action, when, reason, {})


class DualMaTimingFactor(_DailyTimingFactor):
    """双均线交叉择时：短期均线上穿/下穿长期均线时发出买卖信号。"""

    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, fast_window: int = 5, slow_window: int = 20) -> None:
        super().__init__("dual-ma")
        if fast_window <= 0 or fast_window >= slow_window:
            raise ValueError("fast_window must be positive and below slow_window")
        self.fast_window, self.slow_window = fast_window, slow_window

    @property
    def history_lookback_days(self) -> int:
        return self.slow_window + 1  # 需能算 D-1 与 D-2 两根均线以判交叉

    def _bullish(self, closes: list[float]) -> bool:
        fast = sum(closes[-self.fast_window:]) / self.fast_window
        return fast > sum(closes) / len(closes)

    def daily_signal(self, date: Any) -> tuple[dict[str, float], list[SignalIntent]]:
        prices: dict[str, float] = {}
        intents: list[SignalIntent] = []
        need = self.slow_window + 1
        for code, bars in self.history_bars(date).items():
            closes = [float(getattr(b, "close")) for b in bars]
            if len(closes) < need:
                continue
            prices[code] = closes[-1]
            now = self._bullish(closes[-self.slow_window:])
            before = self._bullish(closes[-need:-1])
            if now and not before:
                intents.append(SignalIntent(code, "BUY", date, "dual moving average bullish cross", {}))
            elif before and not now:
                intents.append(SignalIntent(code, "SELL", date, "dual moving average bearish cross", {}))
        return prices, intents

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class BreakoutTimingFactor(_DailyTimingFactor):
    """区间突破择时：收盘价突破前 N 日最高价买入，跌破前 N 日最低价卖出。"""

    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, window: int = 20) -> None:
        super().__init__("breakout")
        self.window = window
        self.history_fields = ("close", "high", "low")

    @property
    def history_lookback_days(self) -> int:
        return self.window + 1

    def daily_signal(self, date: Any) -> tuple[dict[str, float], list[SignalIntent]]:
        prices: dict[str, float] = {}
        intents: list[SignalIntent] = []
        need = self.window + 1
        for code, bars in self.history_bars(date).items():
            if len(bars) < need:
                continue
            prior = bars[:-1]  # D-1-window .. D-2
            cur = bars[-1]     # D-1
            cur_close = float(getattr(cur, "close"))
            prices[code] = cur_close
            if cur_close > max(float(getattr(b, "high")) for b in prior):
                intents.append(self._intent_at(code, date, "BUY", "daily high breakout"))
            elif cur_close < min(float(getattr(b, "low")) for b in prior):
                intents.append(self._intent_at(code, date, "SELL", "daily low breakdown"))
        return prices, intents

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class MeanReversionTimingFactor(_DailyTimingFactor):
    """RSI 均值回归择时：RSI 超卖时买入、超买时卖出，押注价格向均值回归。"""

    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, window: int = 14, oversold: float = 30, overbought: float = 70) -> None:
        super().__init__("mean-reversion")
        self.window, self.oversold, self.overbought = window, oversold, overbought

    @property
    def history_lookback_days(self) -> int:
        return self.window + 1

    @staticmethod
    def _rsi(closes: list[float]) -> float:
        changes = [b - a for a, b in zip(closes, closes[1:])]
        gains = sum(max(change, 0) for change in changes)
        losses = sum(max(-change, 0) for change in changes)
        return 100.0 if losses == 0 else 100.0 - 100.0 / (1.0 + gains / losses)

    def daily_signal(self, date: Any) -> tuple[dict[str, float], list[SignalIntent]]:
        prices: dict[str, float] = {}
        intents: list[SignalIntent] = []
        need = self.window + 1
        for code, bars in self.history_bars(date).items():
            closes = [float(getattr(b, "close")) for b in bars]
            if len(closes) < need:
                continue
            prices[code] = closes[-1]
            rsi = self._rsi(closes)
            if rsi <= self.oversold:
                intents.append(self._intent_at(code, date, "BUY", "RSI oversold reversal"))
            elif rsi >= self.overbought:
                intents.append(self._intent_at(code, date, "SELL", "RSI overbought reversal"))
        return prices, intents

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class GoldenCrossTimingFactor(_DailyTimingFactor):
    """均线多头排列（金叉）择时：快/中/慢均线按序上抬时买入，排列走坏时卖出。"""

    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, fast_window: int = 5, middle_window: int = 10, slow_window: int = 20) -> None:
        super().__init__("golden-cross")
        if not fast_window < middle_window < slow_window:
            raise ValueError("moving-average windows must be ascending")
        self.windows = (fast_window, middle_window, slow_window)

    @property
    def history_lookback_days(self) -> int:
        return self.windows[-1] + 1

    @staticmethod
    def _bullish(numbers: list[float], windows: tuple[int, int, int]) -> bool:
        fast, middle, slow = windows
        return (
            sum(numbers[-fast:]) / fast
            > sum(numbers[-middle:]) / middle
            > sum(numbers[-slow:]) / slow
        )

    def daily_signal(self, date: Any) -> tuple[dict[str, float], list[SignalIntent]]:
        prices: dict[str, float] = {}
        intents: list[SignalIntent] = []
        need = self.windows[-1] + 1
        for code, bars in self.history_bars(date).items():
            closes = [float(getattr(b, "close")) for b in bars]
            if len(closes) < need:
                continue
            prices[code] = closes[-1]
            now = self._bullish(closes, self.windows)
            before = self._bullish(closes[:-1], self.windows)
            if now and not before:
                intents.append(self._intent_at(code, date, "BUY", "golden cross alignment"))
            elif before and not now:
                intents.append(self._intent_at(code, date, "SELL", "golden cross alignment ended"))
        return prices, intents

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class PassiveTimingFactor(_DailyTimingFactor):
    """被动择时：始终不产生任何买卖信号（占位，用于不择时的策略）。"""

    def __init__(self) -> None:
        super().__init__("passive")

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


__all__ = [
    "BreakoutTimingFactor",
    "DualMaTimingFactor",
    "GoldenCrossTimingFactor",
    "MeanReversionTimingFactor",
    "PassiveTimingFactor",
]
