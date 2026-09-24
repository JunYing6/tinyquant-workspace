"""预置的简单策略（Simple Strategies）集合。

每个策略由三段节点拼装而成：
- 选股器(Selector)：决定候选股票池。
- 择时(Timer)：决定何时买卖（K 线因子 + 信号执行因子）。
- 风控(RiskControl)：决定仓位与是否允许交易。

默认股票池为 ["000001.SZ", "600000.SH"]，可通过 stock_pool 覆盖。
"""

from __future__ import annotations

from collections.abc import Sequence

from trading_nodes.methods.selector.fixed import FixedStockPicking
from trading_nodes.methods.timer.passive import NoTradeTiming
from trading_nodes.methods.risk.full_position import FullPositionRisk
from trading_nodes.methods.risk.atr import AtrRiskControl
from trading_nodes.methods.selector.selection import MomentumSelector, PriceFilterSelector
from trading_nodes.factors.timer.tick.intent_executor import IntentExecutorFactor
from trading_nodes.factors.timer.kline.simple_timing import (
    BreakoutTimingFactor,
    DualMaTimingFactor,
    GoldenCrossTimingFactor,
    MeanReversionTimingFactor,
)
from trading_nodes_base.methods import BaseTimeSelection
from trading_nodes_base.strategies import BaseStrategy


def _timing(name: str, factors: list) -> BaseTimeSelection:
    """组装一个择时器：K 线因子 + 统一的信号执行因子。"""
    return BaseTimeSelection(name, factors, [IntentExecutorFactor()])


class DualMaStrategy(BaseStrategy):
    """双均线交叉择时策略：指定股票池 + 双均线买卖 + 满仓。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("dual-ma", selector, _timing("dual-ma-timing", [DualMaTimingFactor()]), FullPositionRisk())


class BreakoutStrategy(BaseStrategy):
    """区间突破择时策略：突破买入/跌破卖出 + 满仓。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("breakout", selector, _timing("breakout-timing", [BreakoutTimingFactor()]), FullPositionRisk())


class MeanReversionStrategy(BaseStrategy):
    """RSI 均值回归策略：超卖买/超买卖 + 满仓。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("mean-reversion", selector, _timing("mean-reversion-timing", [MeanReversionTimingFactor()]), FullPositionRisk())


class GoldenCrossStrategy(BaseStrategy):
    """均线多头排列（金叉）策略：排列成立买入/破坏卖出 + 满仓。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("golden-cross", selector, _timing("golden-cross-timing", [GoldenCrossTimingFactor()]), FullPositionRisk())


class AtrStopStrategy(BaseStrategy):
    """ATR 止损策略：双均线择时 + ATR 止损风控。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("atr-stop", selector, _timing("atr-stop-timing", [DualMaTimingFactor()]), AtrRiskControl())


class BreakoutRiskStrategy(BaseStrategy):
    """区间突破 + ATR 止损策略。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("breakout-risk", selector, _timing("breakout-risk-timing", [BreakoutTimingFactor()]), AtrRiskControl())


class MomentumPickStrategy(BaseStrategy):
    """动量选股策略：动量打分选股 + 被动择时 + 满仓。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = MomentumSelector(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("momentum-pick", selector, NoTradeTiming(), FullPositionRisk())


class FilterPickStrategy(BaseStrategy):
    """价格过滤选股策略：均线过滤选股 + 被动择时 + 满仓。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = PriceFilterSelector(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("filter-pick", selector, NoTradeTiming(), FullPositionRisk())


class EmptyPositionStrategy(BaseStrategy):
    """空仓策略：空股票池，不做任何交易。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking([] if stock_pool is None else stock_pool)
        super().__init__("empty-position", selector, NoTradeTiming(), FullPositionRisk())


__all__ = [
    "AtrStopStrategy",
    "BreakoutRiskStrategy",
    "BreakoutStrategy",
    "DualMaStrategy",
    "EmptyPositionStrategy",
    "FilterPickStrategy",
    "GoldenCrossStrategy",
    "MeanReversionStrategy",
    "MomentumPickStrategy",
]
