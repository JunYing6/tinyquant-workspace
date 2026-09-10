from __future__ import annotations

from data.realtime.mock import MockQuoteClient
from trading_nodes.live import (
    PaperTradeExecutor,
    build_executor,
    build_live,
    build_quote_client,
)
from tools.data import InMemoryGateway
from trading_nodes.strategies.buy_close import BuyCloseStrategy


def test_paper_executor_buy_and_sell_updates_account() -> None:
    executor = PaperTradeExecutor({"initial_cash": 100000.0})
    executor.connect()

    executor.buy("600519", 100, price=10.0)
    positions = executor.get_positions()
    account = executor.get_account()

    assert positions[0]["symbol"] == "600519"
    assert positions[0]["volume"] == 100
    assert account["available_cash"] == 100000.0 - 1000.0

    executor.sell("600519", 50, price=12.0)
    assert executor.get_positions()[0]["volume"] == 50
    assert executor.get_account()["available_cash"] == 100000.0 - 1000.0 + 600.0


def test_build_executor_paper() -> None:
    assert isinstance(build_executor("paper", {}), PaperTradeExecutor)


def test_build_quote_client_mock() -> None:
    assert isinstance(build_quote_client("mock"), MockQuoteClient)


def test_build_live_returns_engine() -> None:
    from engines.realtime import RealTimeTradeEngine

    strategy = BuyCloseStrategy()
    gateway = InMemoryGateway(bars=[], sessions=[])
    engine = build_live(strategy, gateway, route="paper", initial_capital=100000)

    assert isinstance(engine, RealTimeTradeEngine)
