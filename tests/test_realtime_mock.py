from __future__ import annotations

from data.realtime.base import Level1Tick
from data.realtime.mock import MockQuoteClient


def test_mock_quote_client_delivers_ticks() -> None:
    client = MockQuoteClient()
    received = []

    def on_tick(tick: Level1Tick) -> None:
        received.append(tick)

    client.on_tick = on_tick
    client.emit(Level1Tick(code="600519", name="x", time="09:30:00", price=100.0, change=0.0, volume=100, amount=10000.0))

    assert received and received[0].code == "600519"
    assert received[0].price == 100.0