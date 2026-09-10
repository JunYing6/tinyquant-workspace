from __future__ import annotations

from data.adapters.workspace_data import build_workspace_gateway
from data.adapters.workspace_data.realtime import WorkspaceRealtimeAdapter
from data.realtime.base import Level1Tick
from data.realtime.mock import MockQuoteClient
from tools.data import QuoteTick, StreamRequest, TradeTick


def _trade_request() -> StreamRequest:
    return StreamRequest(dataset="market.trade", instruments=("600519",))


def _quote_request() -> StreamRequest:
    return StreamRequest(dataset="market.quote", instruments=("600519",))


def _tick() -> Level1Tick:
    return Level1Tick(
        code="600519", name="x", time="09:31:00", price=100.0, change=0.0,
        volume=100, amount=10000.0, trade_date="20240102",
        bid5=[(10, 99.0)], ask5=[(10, 101.0)],
    )


def test_subscribe_delivers_trade_tick() -> None:
    client = MockQuoteClient()
    adapter = WorkspaceRealtimeAdapter(client)
    received = []
    adapter.subscribe(_trade_request(), received.append)

    client.emit(_tick())

    assert len(received) == 1
    assert isinstance(received[0], TradeTick)
    assert received[0].instrument_id == "600519"
    assert received[0].price == 100.0


def test_subscribe_delivers_quote_tick() -> None:
    client = MockQuoteClient()
    adapter = WorkspaceRealtimeAdapter(client)
    received = []
    adapter.subscribe(_quote_request(), received.append)

    client.emit(_tick())

    assert len(received) == 1
    assert isinstance(received[0], QuoteTick)
    assert received[0].last_price == 100.0
    assert received[0].bid_levels[0].price == 99.0
    assert received[0].ask_levels[0].price == 101.0


def test_subscribe_returns_cancellable_subscription() -> None:
    client = MockQuoteClient()
    adapter = WorkspaceRealtimeAdapter(client)
    received = []
    subscription = adapter.subscribe(_trade_request(), received.append)

    assert subscription.is_active()
    subscription.cancel()
    assert not subscription.is_active()


def test_gateway_subscribe_routes_realtime_tick() -> None:
    client = MockQuoteClient()
    gateway = build_workspace_gateway(data_root=None, realtime_client=client)
    received = []
    gateway.subscribe(
        StreamRequest(dataset="market.trade", instruments=("600519",)),
        received.append,
    )

    client.emit(_tick())

    assert len(received) == 1
    assert isinstance(received[0], TradeTick)
    assert received[0].instrument_id == "600519"