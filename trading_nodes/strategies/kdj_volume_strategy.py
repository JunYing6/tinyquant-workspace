"""KDJ + volume-confirmed breakout strategy.

Mirrors a classic daily-bar trading rule:

- Buy  when the slow KDJ (14,3,3) K-line crosses up over D, AND today's
       volume exceeds the 10-day average volume, AND close is above the
       10-day average close (avgPriceBuy).
- Sell when close drops below the 20-day average close (avgPriceExit).

Designated (fixed) stock picker selects the pool; the timer factor only
trades pool members.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from typing import Any, Dict, List, Optional

from trading_nodes_base.factors import KlineTimingFactor
from trading_nodes_base.methods import BaseTimeSelection
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.types import SignalIntent
from trading_nodes.factors.timer.tick.intent_executor import IntentExecutorFactor
from trading_nodes.methods.risk.full_position import FullPositionRisk
from trading_nodes.methods.selector.fixed import FixedStockPicking


class KdjVolumeTimingFactor(KlineTimingFactor):
    frequency = "1d"
    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(
        self,
        pool: Optional[Sequence[str]] = None,
        *,
        rsv_window: int = 14,
        k_smooth: int = 3,
        d_smooth: int = 3,
        ma_short: int = 10,
        ma_exit: int = 20,
        vol_window: int = 10,
    ) -> None:
        super().__init__("kdj-volume")
        self.pool = set(pool or ())
        self.rsv_window = int(rsv_window)
        self.k_smooth = int(k_smooth)
        self.d_smooth = int(d_smooth)
        self.ma_short = int(ma_short)
        self.ma_exit = int(ma_exit)
        self.vol_window = int(vol_window)
        self._high: Dict[str, deque[float]] = {}
        self._low: Dict[str, deque[float]] = {}
        self._close: Dict[str, deque[float]] = {}
        self._volume: Dict[str, deque[float]] = {}
        self._k: Dict[str, float] = {}
        self._d: Dict[str, float] = {}

    def get_query_lst(self, date: Any, codes: Optional[list[str]] = None) -> list[dict]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    @staticmethod
    def _smooth(previous: float, value: float, period: int) -> float:
        return (previous * (period - 1) + value) / period

    def on_bar(self, bar: Any) -> List[SignalIntent]:
        code = getattr(bar, "code", None)
        if not code or (self.pool and code not in self.pool):
            return []
        highs = self._high.setdefault(code, deque(maxlen=self.rsv_window))
        lows = self._low.setdefault(code, deque(maxlen=self.rsv_window))
        closes = self._close.setdefault(code, deque(maxlen=self.ma_exit))
        volumes = self._volume.setdefault(code, deque(maxlen=self.vol_window))
        highs.append(float(bar.high))
        lows.append(float(bar.low))
        closes.append(float(bar.close))
        volumes.append(float(bar.volume))
        if (
            len(highs) < self.rsv_window
            or len(closes) < self.ma_exit
            or len(volumes) < self.vol_window
        ):
            return []

        highest = max(highs)
        lowest = min(lows)
        span = highest - lowest
        rsv = (float(bar.close) - lowest) / span * 100.0 if span > 0 else 50.0
        old_k = self._k.get(code, 50.0)
        old_d = self._d.get(code, 50.0)
        new_k = self._smooth(old_k, rsv, self.k_smooth)
        new_d = self._smooth(old_d, new_k, self.d_smooth)
        self._k[code] = new_k
        self._d[code] = new_d

        avg_volume = sum(volumes) / len(volumes)
        ma_short = sum(list(closes)[-self.ma_short:]) / self.ma_short
        ma_exit = sum(closes) / len(closes)

        intents: List[SignalIntent] = []
        cross_up = old_k <= old_d and new_k > new_d
        if cross_up and float(bar.volume) > avg_volume and float(bar.close) > ma_short:
            intents.append(SignalIntent(
                code, "BUY", getattr(bar, "end_time", None),
                "kdj-volume-buy", {"sizing_intent": 1.0},
            ))
        if float(bar.close) < ma_exit:
            intents.append(SignalIntent(
                code, "SELL", getattr(bar, "end_time", None),
                "kdj-volume-sell", {"sizing_intent": 1.0},
            ))
        return intents


class KdjVolumeStrategy(BaseStrategy):
    """KDJ cross + volume + MA-breakout buy, 20-day MA exit."""

    supports_fast_backtest = True

    def __init__(self, stock_pool: Optional[Sequence[str]] = None) -> None:
        pool = list(stock_pool or ["000001.SZ", "600000.SH"])
        selector = FixedStockPicking(pool)
        timer = BaseTimeSelection(
            "kdj-volume-timer",
            kline_factors=[KdjVolumeTimingFactor(pool)],
            tick_factors=[IntentExecutorFactor()],
        )
        super().__init__(
            strategy_name="kdj-volume",
            selector=selector,
            timer=timer,
            risk_ctrl=FullPositionRisk(),
        )


__all__ = ["KdjVolumeStrategy", "KdjVolumeTimingFactor"]
