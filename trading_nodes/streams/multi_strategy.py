from __future__ import annotations

from trading_nodes.minds.weighting import PerformanceWeightMind
from trading_nodes.strategies.simple_strategies import (
    AtrStopStrategy,
    BreakoutRiskStrategy,
    BreakoutStrategy,
    DualMaStrategy,
    EmptyPositionStrategy,
    FilterPickStrategy,
    GoldenCrossStrategy,
    MeanReversionStrategy,
    MomentumPickStrategy,
)
from trading_nodes_base.minds import BaseMind
from trading_nodes_base.streams import BaseStream


class MultiStrategyStream(BaseStream):
    def __init__(self, mind: BaseMind | None = None) -> None:
        strategies = [
            DualMaStrategy(),
            BreakoutStrategy(),
            MeanReversionStrategy(),
            GoldenCrossStrategy(),
            AtrStopStrategy(),
            BreakoutRiskStrategy(),
            MomentumPickStrategy(),
            FilterPickStrategy(),
            EmptyPositionStrategy(),
        ]
        super().__init__("multi-strategy", strategies, mind or PerformanceWeightMind())


__all__ = ["MultiStrategyStream"]