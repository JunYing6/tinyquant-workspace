"""Sixty-day in-memory demo dataset for the nine-strategy pack.

``InMemoryStrategyData`` is itself a :class:`InMemoryGateway`: it builds the
canonical ``Bar``/``Session`` envelopes for ``N_DAYS`` trading days and serves
them through the standard ``tools.data`` reading paths.  ``run_multi.py`` feeds
the instance straight into ``FastBacktestEngine`` as ``data_gateway``.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from tools.data import Bar, InMemoryGateway, Session, TradingPhase

CODES = ["000001.SZ", "000002.SZ", "600000.SH", "600036.SH", "600519.SH"]
N_DAYS = 60


def _trade_dates() -> list[str]:
    current = date(2024, 1, 2)
    dates: list[str] = []
    while len(dates) < N_DAYS:
        if current.weekday() < 5:
            dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


class InMemoryStrategyData(InMemoryGateway):
    def __init__(self) -> None:
        self._dates = _trade_dates()
        bars: list[Bar] = []
        sessions: list[Session] = []
        for day in self._dates:
            close = datetime.strptime(day, "%Y%m%d").replace(hour=15, tzinfo=timezone.utc)
            phase = TradingPhase(name="regular", start=close.replace(hour=9), end=close, accepts_trades=True, accepts_quotes=True)
            sessions.append(Session(market="CN", trading_date=close.date(), timezone="UTC", phases=(phase,)))
            for code in CODES:
                bars.append(
                    Bar(
                        schema_version="1",
                        event_id=None,
                        instrument_id=code,
                        asset_type="equity",
                        effective_time=close,
                        event_time=close,
                        available_at=close,
                        trading_date=close.date(),
                        source="demo",
                        quality="valid",
                        metadata={},
                        frequency="1d",
                        interval_start=close.replace(hour=9),
                        interval_end=close,
                        open=10.0,
                        high=11.0,
                        low=9.5,
                        close=10.5,
                        volume=1000.0,
                        turnover=10500.0,
                        is_complete=True,
                        price_basis="raw",
                    )
                )
        super().__init__(bars=bars, sessions=sessions)

    @property
    def dates(self) -> list[str]:
        return list(self._dates)

    def dates_in_range(self, start: str, end: str) -> list[str]:
        return [day for day in self._dates if start <= day <= end]


def demo_data() -> InMemoryStrategyData:
    return InMemoryStrategyData()


__all__ = ["CODES", "N_DAYS", "InMemoryStrategyData", "demo_data"]
