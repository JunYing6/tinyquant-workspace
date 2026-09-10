from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Any, Callable, Iterator, Optional

from tools.data import (
    AdapterDescriptor,
    DatasetCapability,
    PriceLevel,
    QuoteTick,
    RealtimeDataPort,
    StreamEvent,
    StreamRequest,
    Subscription,
    TradeTick,
    default_catalog,
)

from data.realtime.base import Level1Tick

ADAPTER_NAME = "workspace-realtime"
SCHEMA_VERSION = "1"
SOURCE = "workspace-realtime"

_DATASETS = ("market.trade", "market.quote")
_MODES = ("push", "poll")


def _capability(dataset: str) -> DatasetCapability:
    definition = default_catalog().get(dataset)
    return DatasetCapability(
        dataset=dataset,
        modes=_MODES,
        asset_types=frozenset({"equity", "index"}),
        frequencies=(),
        fields=tuple(definition.fields),
        point_in_time=definition.point_in_time,
    )


class WorkspaceRealtimeAdapter:
    _SINK_CODES: set[str] = set()

    def __init__(self, quote_client: Any, market: str = "CN") -> None:
        self.quote_client = quote_client
        self.market = market
        self.descriptor = AdapterDescriptor(
            name=ADAPTER_NAME,
            datasets={dataset: _capability(dataset) for dataset in _DATASETS},
            historical_modes=(),
            realtime_modes=_MODES,
            supports_point_in_time=True,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset({"equity", "index"}),
            schema_versions=(SCHEMA_VERSION,),
            ordering_guarantee="per_instrument",
        )
        self._sinks: dict[str, Optional[Callable[[StreamEvent], None]]] = {
            dataset: None for dataset in _DATASETS
        }

    def subscribe(
        self,
        request: StreamRequest,
        sink: Callable[[StreamEvent], None],
        control_sink: Optional[Callable[[StreamEvent], None]] = None,
    ) -> Subscription:
        self._sinks[request.dataset] = sink
        instruments = tuple(request.instruments)
        self._SINK_CODES.update(instruments)
        self.quote_client.subscribe(*instruments)
        self.quote_client.on_tick = self._make_on_tick(request.dataset)
        return Subscription()

    def poll(self, request: StreamRequest) -> Iterator[StreamEvent]:
        yield from ()

    def _make_on_tick(self, dataset: str) -> Callable[[Level1Tick], None]:
        def _on_tick(tick: Level1Tick) -> None:
            event = self._to_event(dataset, tick)
            if event is None:
                return
            sink = self._sinks.get(dataset)
            if sink is not None:
                sink(event)

        return _on_tick

    def _to_event(self, dataset: str, tick: Level1Tick) -> Optional[StreamEvent]:
        common = {
            "schema_version": SCHEMA_VERSION,
            "event_id": None,
            "instrument_id": tick.code,
            "asset_type": "equity",
            "effective_time": None,
            "event_time": _parse_time(tick.trade_date, tick.time),
            "available_at": None,
            "trading_date": _parse_date(tick.trade_date),
            "source": SOURCE,
            "quality": "valid",
            "metadata": {},
        }
        if dataset == "market.trade":
            return TradeTick(
                event_type="trade",
                price=tick.price,
                size=float(tick.volume),
                turnover=float(tick.amount),
                side="UNKNOWN",
                sequence=0,
                **common,
            )
        if dataset == "market.quote":
            return QuoteTick(
                event_type="quote",
                bid_levels=_levels(tick.bid5, descending=True),
                ask_levels=_levels(tick.ask5, descending=False),
                last_price=tick.price,
                last_size=float(tick.volume),
                sequence=0,
                **common,
            )
        return None


def _levels(rows: list[tuple[int, float]], descending: bool) -> tuple[PriceLevel, ...]:
    levels = tuple(
        PriceLevel(price=price, size=float(size), level=i + 1)
        for i, (size, price) in enumerate(rows)
    )
    if descending:
        levels = tuple(sorted(levels, key=lambda level: level.price, reverse=True))
    else:
        levels = tuple(sorted(levels, key=lambda level: level.price))
    return levels


def _parse_date(value: str | None) -> Any:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        return None


def _parse_time(day: str | None, clock: str) -> Any:
    date_part = _parse_date(day) or datetime.now(timezone.utc).date()
    hour, minute, second = 9, 30, 0
    parts = clock.split(":")
    if len(parts) > 0 and parts[0].isdigit():
        hour = int(parts[0])
    if len(parts) > 1:
        minute = int(parts[1])
    if len(parts) > 2:
        second = int(parts[2])
    return datetime.combine(date_part, time(hour, minute, second), tzinfo=timezone.utc)


__all__ = ["ADAPTER_NAME", "WorkspaceRealtimeAdapter"]