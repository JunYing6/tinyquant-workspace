"""Example adapters implementing the release ``tools.data`` Ports.

This demonstrates how a data source (here: in-memory mappings) is adapted to the
canonical ``DataRequest``/``DataBatch``/``Session`` contract.  To connect a real
source, implement the same Ports and translate your files/database/API rows into
``tools.data`` records.  Each adapter exposes a ``descriptor`` so the
``DataGateway`` can route by dataset and capability.

All datetimes in this module are timezone-aware in UTC; the gateway policy must
therefore be assembled with ``timezone="UTC"`` (see ``main.build_gateway``).
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any, Iterator, Mapping

from tools.data import (
    AdapterDescriptor,
    Bar,
    CalendarBatch,
    CalendarRequest,
    DataBatch,
    DataProvenance,
    DataRequest,
    DatasetCapability,
    QualityReport,
    Session,
    TradingPhase,
    default_catalog,
)

SCHEMA_VERSION = "1"
ADAPTER_NAME_HISTORICAL = "example-historical"
ADAPTER_NAME_CALENDAR = "example-calendar"
_READ_AT = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _utc(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc)


class InMemoryHistoricalAdapter:
    """Serves ``market.bar`` records from an in-memory mapping.

    ``data`` maps a ``YYYYMMDD`` day to ``{code: {open, high, low, close}}``.
    """

    def __init__(self, data: Mapping[str, Mapping[str, Mapping[str, Any]]]) -> None:
        self._data = data
        bar_def = default_catalog().get("market.bar")
        self.descriptor = AdapterDescriptor(
            name=ADAPTER_NAME_HISTORICAL,
            datasets={
                "market.bar": DatasetCapability(
                    dataset="market.bar",
                    modes=("historical",),
                    asset_types=frozenset(bar_def.asset_types),
                    frequencies=("1d",),
                    fields=tuple(bar_def.fields),
                    point_in_time=True,
                    ordering_guarantee="per_instrument",
                )
            },
            historical_modes=("historical",),
            realtime_modes=(),
            supports_point_in_time=True,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset(bar_def.asset_types),
            schema_versions=(SCHEMA_VERSION,),
            ordering_guarantee="per_instrument",
        )

    def read(self, request: DataRequest) -> DataBatch[Bar]:
        rows: list[Bar] = []
        for day, mapping in sorted(self._data.items()):
            if not self._in_range(day, request):
                continue
            for code, ohlc in mapping.items():
                rows.append(self._build_bar(day, code, ohlc))
        rows.sort(key=lambda bar: (bar.instrument_id or "", bar.interval_start))
        return self._batch(request, rows)

    def iter(self, request: DataRequest, chunk_size: int = 10_000) -> Iterator[DataBatch[Bar]]:
        yield self.read(request)

    def _build_bar(self, day: str, code: str, ohlc: Mapping[str, Any]) -> Bar:
        day_d = date.fromisoformat(day)
        session_end = _utc(day_d, 7, 0)
        return Bar(
            schema_version=SCHEMA_VERSION,
            event_id=None,
            instrument_id=code,
            asset_type="equity",
            effective_time=session_end,
            event_time=session_end,
            available_at=session_end,
            trading_date=day_d,
            source="example",
            quality="valid",
            metadata={},
            frequency="1d",
            interval_start=_utc(day_d, 1, 30),
            interval_end=session_end,
            open=float(ohlc["open"]),
            high=float(ohlc["high"]),
            low=float(ohlc["low"]),
            close=float(ohlc["close"]),
            volume=0.0,
            turnover=0.0,
            is_complete=True,
            price_basis="raw",
        )

    def _batch(self, request: DataRequest, records: list[Bar]) -> DataBatch[Bar]:
        return DataBatch(
            request_id=request.correlation_id or "",
            dataset="market.bar",
            schema_version=SCHEMA_VERSION,
            correlation_id=request.correlation_id,
            records=tuple(records),
            complete=True,
            next_cursor=None,
            provenance=DataProvenance(
                adapter_name=ADAPTER_NAME_HISTORICAL,
                source_revision="static",
                request_fingerprint=request.delivery_key or "example",
                read_at=_READ_AT,
            ),
            quality=QualityReport(status="ok", checked_count=len(records)),
        )

    @staticmethod
    def _in_range(day: str, request: DataRequest) -> bool:
        start = request.anchor or request.start
        end = request.end
        if start is None:
            return True
        start_s = start.strftime("%Y%m%d") if hasattr(start, "strftime") else str(start)
        end_s = end.strftime("%Y%m%d") if end else start_s
        return start_s <= day <= end_s


class InMemoryCalendarAdapter:
    """Returns a single canonical CN session for every requested trading day."""

    def __init__(self) -> None:
        session_def = default_catalog().get("calendar.session")
        self.descriptor = AdapterDescriptor(
            name=ADAPTER_NAME_CALENDAR,
            datasets={
                "calendar.session": DatasetCapability(
                    dataset="calendar.session",
                    modes=("calendar",),
                    asset_types=frozenset(session_def.asset_types),
                    frequencies=(),
                    fields=(),
                    point_in_time=False,
                )
            },
            historical_modes=(),
            realtime_modes=(),
            supports_point_in_time=False,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset(session_def.asset_types),
            schema_versions=(SCHEMA_VERSION,),
        )

    def sessions(self, request: CalendarRequest) -> CalendarBatch:
        sessions = []
        day = request.start
        while day <= request.end:
            sessions.append(
                Session(
                    market=request.market,
                    trading_date=day,
                    timezone="UTC",
                    phases=(
                        TradingPhase(
                            name="main",
                            start=_utc(day, 1, 30),
                            end=_utc(day, 7, 0),
                            accepts_trades=True,
                            accepts_quotes=True,
                        ),
                    ),
                )
            )
            day = date.fromordinal(day.toordinal() + 1)
        return CalendarBatch(
            request_id="calendar",
            dataset="calendar.session",
            schema_version=SCHEMA_VERSION,
            correlation_id=None,
            records=tuple(sessions),
            complete=True,
            next_cursor=None,
            provenance=DataProvenance(
                adapter_name=ADAPTER_NAME_CALENDAR,
                source_revision="static",
                request_fingerprint=request.market,
                read_at=_READ_AT,
            ),
            quality=QualityReport(status="ok", checked_count=len(sessions)),
        )
