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

from collections.abc import Sequence
from typing import Any, Dict, List, Optional

from trading_nodes_base.factors import NonIntradayKlineFactor
from trading_nodes_base.methods import BaseTimeSelection
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.types import SignalIntent
from trading_nodes.methods.risk.full_position import FullPositionRisk
from trading_nodes.methods.selector.fixed import FixedStockPicking


class KdjVolumeTimingFactor(NonIntradayKlineFactor):
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
        self.history_fields = ("high", "low", "close", "volume")

    @property
    def history_lookback_days(self) -> int:
        return max(self.rsv_window, self.ma_exit, self.vol_window) + 1

    def get_query_lst(self, date: Any, codes: Optional[list[str]] = None) -> list[dict]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    @staticmethod
    def _smooth(previous: float, value: float, period: int) -> float:
        return (previous * (period - 1) + value) / period

    def daily_signal(self, date: Any) -> tuple[Dict[str, float], List[SignalIntent]]:
        prices: Dict[str, float] = {}
        intents: List[SignalIntent] = []
        need = max(self.rsv_window, self.ma_exit, self.vol_window)
        for code, bars in self.history_bars(date).items():
            if self.pool and code not in self.pool:
                continue
            if len(bars) < need:
                continue
            k, d = 50.0, 50.0
            old_k, old_d = k, d
            closes = [float(getattr(b, "close")) for b in bars]
            volumes = [float(getattr(b, "volume")) for b in bars]
            for i, bar in enumerate(bars):
                window = bars[max(0, i - self.rsv_window + 1):i + 1]
                highest = max(float(getattr(b, "high")) for b in window)
                lowest = min(float(getattr(b, "low")) for b in window)
                span = highest - lowest
                rsv = (closes[i] - lowest) / span * 100.0 if span > 0 else 50.0
                old_k, old_d = k, d
                k = self._smooth(old_k, rsv, self.k_smooth)
                d = self._smooth(old_d, k, self.d_smooth)
            prices[code] = closes[-1]
            avg_volume = sum(volumes[-self.vol_window:]) / self.vol_window
            ma_short = sum(closes[-self.ma_short:]) / self.ma_short
            ma_exit = sum(closes[-self.ma_exit:]) / self.ma_exit
            cross_up = old_k <= old_d and k > d
            if cross_up and volumes[-1] > avg_volume and closes[-1] > ma_short:
                intents.append(SignalIntent(code, "BUY", date, "kdj-volume-buy", {"sizing_intent": 1.0}))
            if closes[-1] < ma_exit:
                intents.append(SignalIntent(code, "SELL", date, "kdj-volume-sell", {}))
        return prices, intents

    def on_bar(self, bar: Any) -> List[SignalIntent]:
        return []


class KdjVolumeStrategy(BaseStrategy):
    """KDJ cross + volume + MA-breakout buy, 20-day MA exit."""

    supports_fast_backtest = True
    tick_mode = False
    execution_timing = "close"
    fast_buy_timing = "open"
    fast_sell_timing = "open"

    def __init__(self, stock_pool: Optional[Sequence[str]] = None) -> None:
        pool = list(stock_pool or ["000001.SZ", "600000.SH"])
        selector = FixedStockPicking(pool)
        timer = BaseTimeSelection(
            "kdj-volume-timer",
            kline_factors=[KdjVolumeTimingFactor(pool)],
            tick_factors=[],
        )
        super().__init__(
            strategy_name="kdj-volume",
            selector=selector,
            timer=timer,
            risk_ctrl=FullPositionRisk(),
        )


__all__ = ["KdjVolumeStrategy", "KdjVolumeTimingFactor"]
