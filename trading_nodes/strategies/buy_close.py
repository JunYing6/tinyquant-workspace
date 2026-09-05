"""Example user-side strategy.

Demonstrates the canonical fast-backtest execution path:

  1. a ``KlineTimingFactor`` emits a ``SignalIntent`` from ``on_bar``;
  2. the timer queues it; the engine prices it on the next source bar;
  3. ``IntentExecutorFactor`` converts intents into executable orders.

Extend this file (or add new files in this package) with your own logic.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, List

from tools.data import DataRequest, TableBatch
from trading_nodes_base.factors.base import KlineTimingFactor
from trading_nodes.factors.timer.kline.simple_timing import PassiveTimingFactor
from trading_nodes.factors.timer.tick.intent_executor import IntentExecutorFactor
from trading_nodes_base.factors.types import SignalIntent
from trading_nodes_base.methods.base import BaseTimeSelection
from trading_nodes.methods.risk.full_position import FullPositionRisk
from trading_nodes.methods.selector.fixed import FixedStockPicking
from trading_nodes_base.strategies.base import BaseStrategy


class BuyFirstBarFactor(KlineTimingFactor):
    """Emit a single BUY intent for each target code on its first bar."""

    emitted_actions = frozenset({"BUY"})

    def __init__(self) -> None:
        super().__init__("buy-first-bar")
        self._fired: set[str] = set()

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[Any]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: Any) -> List[SignalIntent]:
        code = getattr(bar, "instrument_id", None)
        if not isinstance(code, str) or code in self._fired:
            return []
        self._fired.add(code)
        return [
            SignalIntent(
                code,
                "BUY",
                getattr(bar, "interval_end", None),
                "example: buy on first bar",
                {"volume": 100},
            )
        ]


class BuyCloseStrategy(BaseStrategy):
    """Buy each pool member once on its first source bar."""

    supports_fast_backtest = True

    def __init__(self, stock_pool: list[str] | None = None) -> None:
        pool = ["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool
        selector = FixedStockPicking(pool)
        timer = BaseTimeSelection(
            f"{self.__class__.__name__}-timer",
            [BuyFirstBarFactor(), PassiveTimingFactor()],
            [IntentExecutorFactor()],
        )
        super().__init__(
            strategy_name="buy-close",
            selector=selector,
            timer=timer,
            risk_ctrl=FullPositionRisk(),
        )
        self._stock_pool = pool

    def prepare_requirements(self, date: datetime) -> List[DataRequest]:
        """Ask for the daily OHLC of the pool on ``date``."""
        return [
            DataRequest(
                dataset="market.bar",
                anchor=date.date(),
                session_window=(0, 0),
                instruments=tuple(self._stock_pool),
                fields=("open", "high", "low", "close"),
                frequency="1d",
                delivery_key="daily",
            )
        ]

    def receive_data(self, delivery_key: str, batch: TableBatch) -> None:
        """The K-line factor already receives bars via the timer; nothing to do."""
