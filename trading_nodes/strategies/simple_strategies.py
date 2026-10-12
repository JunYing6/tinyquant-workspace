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
from trading_nodes.methods.timer.kline import (
    BreakoutTiming,
    DualMaTiming,
    GoldenCrossTiming,
    MeanReversionTiming,
)
from trading_nodes.methods.timer.passive import NoTradeTiming
from trading_nodes.methods.risk.full_position import FullPositionRisk
from trading_nodes.methods.risk.atr import AtrRiskControl
from trading_nodes_base.strategies import BaseStrategy


class DualMaStrategy(BaseStrategy):
    """双均线交叉择时策略：指定股票池 + 双均线买卖 + 满仓。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("dual-ma", selector, DualMaTiming(), FullPositionRisk())


class BreakoutStrategy(BaseStrategy):
    """区间突破择时策略：突破买入/跌破卖出 + 满仓。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("breakout", selector, BreakoutTiming(), FullPositionRisk())


class MeanReversionStrategy(BaseStrategy):
    """RSI 均值回归策略：超卖买/超买卖 + 满仓。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("mean-reversion", selector, MeanReversionTiming(), FullPositionRisk())


class GoldenCrossStrategy(BaseStrategy):
    """均线多头排列（金叉）策略：排列成立买入/破坏卖出 + 满仓。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("golden-cross", selector, GoldenCrossTiming(), FullPositionRisk())


class AtrStopStrategy(BaseStrategy):
    """ATR 止损策略：双均线择时 + ATR 止损风控。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("atr-stop", selector, DualMaTiming(), AtrRiskControl())


class BreakoutRiskStrategy(BaseStrategy):
    """区间突破 + ATR 止损策略。"""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("breakout-risk", selector, BreakoutTiming(), AtrRiskControl())


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
    "GoldenCrossStrategy",
    "MeanReversionStrategy",
]
