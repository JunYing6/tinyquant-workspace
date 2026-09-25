from __future__ import annotations

from collections.abc import Sequence

from trading_nodes.methods.selector.fixed import FixedStockPicking
from trading_nodes.methods.timer.kline import DualMaTiming
from trading_nodes.methods.risk.full_position import FullPositionRisk
from trading_nodes_base.strategies import BaseStrategy

class ExampleStrategy(BaseStrategy):
    """双均线策略"""
    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        super().__init__("example", FixedStockPicking(["000001.SZ"] if stock_pool is None else stock_pool),
                         DualMaTiming(),FullPositionRisk())
