"""Assemble a user-side DataGateway and run a backtest.

This is the recommended entry point for a user project:

1. put your strategies under ``src/strategies/``,
2. implement your data adapters under ``adapters/``,
3. bind them into a ``DataGateway`` here and feed it to the engine.

Run:

    D:\\Apps\\Python312\\python.exe main.py
"""

from __future__ import annotations

from adapters.memory_adapters import InMemoryCalendarAdapter, InMemoryHistoricalAdapter
from config import settings
from engines.fast import FastBacktestEngine
from trading_nodes.strategies.buy_close import BuyCloseStrategy
from trading_nodes_base.strategies.base import BaseStrategy
from tools.data import DataBinding, DataGateway, DataPolicy, default_catalog

# --- sample daily OHLC data ------------------------------------------------

SAMPLE_DAILY = {
    "20240102": {
        "000001.SZ": {"open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2},
        "600000.SH": {"open": 8.0, "high": 8.4, "low": 7.9, "close": 8.2},
    },
    "20240103": {
        "000001.SZ": {"open": 10.2, "high": 10.6, "low": 10.0, "close": 10.4},
        "600000.SH": {"open": 8.2, "high": 8.6, "low": 8.1, "close": 8.3},
    },
}


def build_gateway() -> DataGateway:
    """Bind the example adapters into a :class:`DataGateway`.

    ``DataBinding.adapter`` must equal ``adapter.descriptor.name``; the adapter
    object is passed separately so the gateway can resolve routes and call the
    right Port.  Swap these two adapters for your own source adapters and the
    strategy + engine stay unchanged.
    """
    historical = InMemoryHistoricalAdapter(SAMPLE_DAILY)
    calendar = InMemoryCalendarAdapter()
    return DataGateway(
        catalog=default_catalog(),
        bindings=[
            (DataBinding("market.bar", historical.descriptor.name, 1, ("historical",)), historical),
            (DataBinding("calendar.session", calendar.descriptor.name, 1, ("calendar",)), calendar),
        ],
        policy=DataPolicy(strict=False),
    )


def build_backtest() -> tuple[BaseStrategy, DataGateway]:
    return BuyCloseStrategy(list(settings.STOCK_POOL)), build_gateway()


def main() -> int:
    strategy, gateway = build_backtest()
    engine = FastBacktestEngine(
        strategy,
        settings.BACKTEST_START,
        settings.BACKTEST_END,
        initial_capital=settings.INITIAL_CAPITAL,
        data_gateway=gateway,
        mode="fast",
        progress_bar=False,
    )
    engine.run()
    stats = engine.get_stats()
    for key, value in sorted(stats.items()):
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
