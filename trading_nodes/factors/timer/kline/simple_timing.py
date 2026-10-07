"""日线 K 线择时因子集合。

这些因子均为"流式因子"：不声明批量数据查询（get_query_lst 返回空），
而是由引擎逐根推送日 K 线(KlineBar)，在 on_bar() 中实时计算买卖信号
(SignalIntent)，用于决定何时进/出股票。基类 _DailyTimingFactor 统一了
"无需查询、直接就绪"的声明约定。
"""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from typing import Any

from trading_nodes_base.factors import KlineTimingFactor
from trading_nodes_base.types import KlineBar, SignalIntent


class _DailyTimingFactor(KlineTimingFactor):
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


class DualMaTimingFactor(_DailyTimingFactor):
    """双均线交叉择时：短期均线上穿/下穿长期均线时发出买卖信号。"""

    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, fast_window: int = 5, slow_window: int = 20) -> None:
        super().__init__("dual-ma")
        if fast_window <= 0 or fast_window >= slow_window:
            raise ValueError("fast_window must be positive and below slow_window")
        self.fast_window, self.slow_window = fast_window, slow_window
        self.history: dict[str, deque[float]] = {}  # 每只股票近 slow_window 根收盘价
        self.previous: dict[str, bool] = {}  # 上一根是否处于多头状态（用于捕捉交叉时刻）

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        values = self.history.setdefault(bar.code, deque(maxlen=self.slow_window))
        values.append(bar.close)
        if len(values) < self.slow_window:
            return []  # 历史数据不足，尚不能计算均线
        # 多头：快速均线 > 慢速均线
        bullish = sum(list(values)[-self.fast_window:]) / self.fast_window > sum(values) / len(values)
        previous = self.previous.get(bar.code, bullish)
        self.previous[bar.code] = bullish
        if bullish and not previous:  # 由空转多 -> 金叉买入
            return [self._intent(bar, "BUY", "dual moving average bullish cross")]
        if previous and not bullish:  # 由多转空 -> 死叉卖出
            return [self._intent(bar, "SELL", "dual moving average bearish cross")]
        return []


class BreakoutTimingFactor(_DailyTimingFactor):
    """区间突破择时：收盘价突破前 N 日最高价买入，跌破前 N 日最低价卖出。"""

    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, window: int = 20) -> None:
        super().__init__("breakout")
        self.window = window
        self.history: dict[str, deque[float]] = {}

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        values = self.history.setdefault(bar.code, deque(maxlen=self.window + 1))
        prior = list(values)  # 不含当前根的近 window 根收盘价
        output: list[SignalIntent] = []
        if len(prior) >= self.window:
            if bar.close > max(prior[-self.window:]):
                output.append(self._intent(bar, "BUY", "daily high breakout"))
            elif bar.close < min(prior[-self.window:]):
                output.append(self._intent(bar, "SELL", "daily low breakdown"))
        values.append(bar.close)
        return output


class MeanReversionTimingFactor(_DailyTimingFactor):
    """RSI 均值回归择时：RSI 超卖时买入、超买时卖出，押注价格向均值回归。"""

    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, window: int = 14, oversold: float = 30, overbought: float = 70) -> None:
        super().__init__("mean-reversion")
        self.window, self.oversold, self.overbought = window, oversold, overbought
        self.history: dict[str, deque[float]] = {}

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        values = self.history.setdefault(bar.code, deque(maxlen=self.window + 1))
        values.append(bar.close)
        if len(values) <= self.window:
            return []
        # 用涨跌幅近似计算 RSI：上涨幅度之和 / 涨跌总幅度
        changes = [b - a for a, b in zip(values, list(values)[1:])]
        gains = sum(max(change, 0) for change in changes)
        losses = sum(max(-change, 0) for change in changes)
        rsi = 100.0 if losses == 0 else 100.0 - 100.0 / (1.0 + gains / losses)
        if rsi <= self.oversold:  # 超卖 -> 看涨买入
            return [self._intent(bar, "BUY", "RSI oversold reversal")]
        if rsi >= self.overbought:  # 超买 -> 看跌卖出
            return [self._intent(bar, "SELL", "RSI overbought reversal")]
        return []


class GoldenCrossTimingFactor(_DailyTimingFactor):
    """均线多头排列（金叉）择时：快/中/慢均线按序上抬时买入，排列走坏时卖出。"""

    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, fast_window: int = 5, middle_window: int = 10, slow_window: int = 20) -> None:
        super().__init__("golden-cross")
        if not fast_window < middle_window < slow_window:
            raise ValueError("moving-average windows must be ascending")
        self.windows = (fast_window, middle_window, slow_window)
        self.history: dict[str, deque[float]] = {}
        self.previous: dict[str, bool] = {}

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        values = self.history.setdefault(bar.code, deque(maxlen=self.windows[-1]))
        values.append(bar.close)
        if len(values) < self.windows[-1]:
            return []
        numbers = list(values)
        fast, middle, slow = self.windows
        # 多头排列：快均线 > 中均线 > 慢均线
        bullish = (
            sum(numbers[-fast:]) / fast
            > sum(numbers[-middle:]) / middle
            > sum(numbers[-slow:]) / slow
        )
        previous = self.previous.get(bar.code, bullish)
        self.previous[bar.code] = bullish
        if bullish and not previous:  # 刚进入多头排列 -> 买入
            return [self._intent(bar, "BUY", "golden cross alignment")]
        if previous and not bullish:  # 多头排列被破坏 -> 卖出
            return [self._intent(bar, "SELL", "golden cross alignment ended")]
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
