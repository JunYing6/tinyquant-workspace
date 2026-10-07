"""被动（不交易）择时。

提供一个始终不产生任何买卖信号的择时器 NoTradeTiming，搭配一个"无操作"
K 线因子 NoTradeFactor。用于不需要主动择时的策略（例如纯选股/纯持仓型策略）。
"""

from __future__ import annotations

from typing import Any

from trading_nodes_base.factors import KlineTimingFactor
from trading_nodes_base.types import KlineBar, SignalIntent
from trading_nodes_base.methods import BaseTimeSelection


class NoTradeFactor(KlineTimingFactor):
    """占位 K 线择时因子：永不产生买卖信号。"""

    def __init__(self) -> None:
        super().__init__("no-trade")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[Any]:
        # 流式因子：无需批量数据查询，直接就绪。
        self._data_clear()
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class NoTradeTiming(BaseTimeSelection):
    """被动择时器：无操作因子（无 tick 因子，K线意图由策略统一提交）。"""

    def __init__(self) -> None:
        super().__init__("no-trade-timing", [NoTradeFactor()], [])


__all__ = ["NoTradeFactor", "NoTradeTiming"]
