from __future__ import annotations

from datetime import date, timedelta

from tools.data.memory import InMemoryGateway


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


class InMemoryStrategyData:
    def __init__(self) -> None:
        self._dates = _trade_dates()
        self.gateway = InMemoryGateway(
            {
                day: [
                    {"code": code, "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "volume": 1000, "amount": 10500.0}
                    for code in CODES
                ]
                for day in self._dates
            }
        )

    @property
    def dates(self) -> list[str]:
        return list(self._dates)

    def dates_in_range(self, start: str, end: str) -> list[str]:
        return [day for day in self._dates if start <= day <= end]


def demo_data() -> InMemoryStrategyData:
    return InMemoryStrategyData()


__all__ = ["CODES", "N_DAYS", "InMemoryStrategyData", "demo_data"]
