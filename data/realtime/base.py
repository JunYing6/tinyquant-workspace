from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol


@dataclass
class Level1Tick:
    code: str
    name: str
    time: str
    price: float
    change: float
    volume: int
    amount: float
    bid5: list[tuple[int, float]] = field(default_factory=list)
    ask5: list[tuple[int, float]] = field(default_factory=list)
    trade_date: Optional[str] = None


class QuoteClient(Protocol):
    def subscribe(self, *codes: str) -> None:
        ...

    def unsubscribe(self, *codes: str) -> None:
        ...

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...

    @property
    def on_tick(self) -> Callable[[Level1Tick], None] | None:
        ...

    @on_tick.setter
    def on_tick(self, fn: Callable[[Level1Tick], None]) -> None:
        ...


__all__ = ["Level1Tick", "QuoteClient"]