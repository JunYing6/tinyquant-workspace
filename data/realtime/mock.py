from __future__ import annotations

from typing import Callable, Optional

from data.realtime.base import Level1Tick


class MockQuoteClient:
    def __init__(self) -> None:
        self._on_tick: Callable[[Level1Tick], None] | None = None
        self._subscribed: set[str] = set()
        self._running = False

    @property
    def on_tick(self) -> Callable[[Level1Tick], None] | None:
        return self._on_tick

    @on_tick.setter
    def on_tick(self, fn: Callable[[Level1Tick], None]) -> None:
        self._on_tick = fn

    @property
    def subscribed_codes(self) -> tuple[str, ...]:
        return tuple(sorted(self._subscribed))

    def subscribe(self, *codes: str) -> None:
        self._subscribed.update(codes)

    def unsubscribe(self, *codes: str) -> None:
        self._subscribed.difference_update(codes)

    def start(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    def emit(self, *ticks: Level1Tick) -> None:
        for tick in ticks:
            if self._on_tick is not None:
                self._on_tick(tick)


__all__ = ["MockQuoteClient"]