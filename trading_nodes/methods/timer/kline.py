"""K 线择时方法节点集合。

每个择时节点在 methods 层把对应的 K 线择时因子组装成一个 BaseTimeSelection，
供策略直接引用（K线意图由策略统一提交给撮合器，无需 tick 执行因子）。
策略层只应从 methods 导入这些节点，而不再自行拼接 BaseTimeSelection。
"""

from __future__ import annotations

from trading_nodes.factors.timer.kline.simple_timing import (
    BreakoutTimingFactor,
    DualMaTimingFactor,
    GoldenCrossTimingFactor,
    MeanReversionTimingFactor,
)
from trading_nodes_base.methods import BaseTimeSelection


class DualMaTiming(BaseTimeSelection):
    """双均线交叉择时节点：双均线因子 + 信号执行因子。"""

    def __init__(self) -> None:
        super().__init__("dual-ma-timing", [DualMaTimingFactor()], [])


class BreakoutTiming(BaseTimeSelection):
    """区间突破择时节点：突破因子 + 信号执行因子。"""

    def __init__(self) -> None:
        super().__init__("breakout-timing", [BreakoutTimingFactor()], [])


class MeanReversionTiming(BaseTimeSelection):
    """RSI 均值回归择时节点：均值回归因子 + 信号执行因子。"""

    def __init__(self) -> None:
        super().__init__("mean-reversion-timing", [MeanReversionTimingFactor()], [])


class GoldenCrossTiming(BaseTimeSelection):
    """均线多头排列（金叉）择时节点：金叉因子 + 信号执行因子。"""

    def __init__(self) -> None:
        super().__init__("golden-cross-timing", [GoldenCrossTimingFactor()], [])


__all__ = [
    "BreakoutTiming",
    "DualMaTiming",
    "GoldenCrossTiming",
    "MeanReversionTiming",
]