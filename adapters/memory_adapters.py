"""Example adapters implementing the release ``tools.data`` Ports.

This demonstrates how a data source (here: in-memory mappings) is adapted to the
canonical ``DataRequest``/``DataBatch``/``Session`` contract.  To connect a real
source, implement the same Ports and translate your files/database/API rows into
``tools.data`` records.  Each adapter exposes a ``descriptor`` so the
``DataGateway`` can route by dataset and capability.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any, Iterable, Iterator, Mapping

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
)

_COMMON = dict(schema_version="1.0", quality="valid", metadata={})


class InMemoryHistoricalAdapter:
    """Serves ``market.bar`` records from an in-memory mapping.

    ``data`` maps a ``YYYYMMDD`` day to ``{code: {open, high, low, close}}``.
    """

    def __init__(self, data: Mapping[str, Mapping[str, Mapping[str, Any]]]) -> None:
        self._data = data
        self.descriptor = AdapterDescriptor(
            name="example-historical",
            datasets={
                "market.bar": DatasetCapability(
                    dataset="market.bar",
                    modes=("historical",),
                    frequencies=("1d",),
                    fields=("open", "high", "low", "close", "instrument_id"),
                    point_in_time=True,
                )
            },
            historical_modes=("read", "iter"),
            supports_point_in_time=True,
        )

    def read(self, request: DataRequest) -> DataBatch:
        rows: list[Bar] = []
        for day, mapping in self._data.items():
            if not self._in_range(day, request):
                continue
            for code, ohlc in mapping.items():
                rows.append(
                    Bar(
                        **_COMMON,
                        event_id=f"{code}-{day}",
                        instrument_id=code,
                        asset_type="equity",
                        effective_time=date.fromisoformat(day),
                        event_time=self._session_end(day),
                        available_at=self._session_end(day),
                        trading_date=date.fromisoformat(day),
                        source="example",
                        frequency="1d",
                        interval_start=self._session_start(day),
                        interval_end=self._session_end(day),
                        open=float(ohlc["open"]),
                        high=float(ohlc["high"]),
                        low=float(ohlc["low"]),
                        close=float(ohlc["close"]),
                        volume=0.0,
                        turnover=0.0,
                        is_complete=True,
                        price_basis="raw",
                    )
                )
        return DataBatch(
            request_id="batch",
            dataset=request.dataset,
            schema_version="1.0",
            correlation_id=None,
            records=tuple(rows),
            complete=True,
            next_cursor=None,
            provenance=DataProvenance(
                adapter_name="example-historical",
                source_revision="static",
                request_fingerprint="example",
                read_at=datetime.now(timezone.utc),
            ),
            quality=QualityReport(status="ok"),
        )

    def iter(self, request: DataRequest, chunk_size: int = 10_000) -> Iterator[DataBatch]:
        yield self.read(request)

    @staticmethod
    def _in_range(day: str, request: DataRequest) -> bool:
        start = request.anchor or request.start
        end = request.end
        if start is None:
            return True
        start_s = start.strftime("%Y%m%d") if hasattr(start, "strftime") else str(start)
        end_s = end.strftime("%Y%m%d") if end else start_s
        return start_s <= day <= end_s

    @staticmethod
    def _session_start(day: str) -> datetime:
        return datetime.combine(date.fromisoformat(day), time(1, 30, tzinfo=timezone.utc))

    @staticmethod
    def _session_end(day: str) -> datetime:
        return datetime.combine(date.fromisoformat(day), time(7, 0, tzinfo=timezone.utc))


class InMemoryCalendarAdapter:
    """Returns a single canonical CN session for every requested trading day."""

    def __init__(self) -> None:
        self.descriptor = AdapterDescriptor(
            name="example-calendar",
            datasets={
                "calendar.session": DatasetCapability(
                    dataset="calendar.session",
                    modes=("calendar",),
                )
            },
        )

    def sessions(self, request: CalendarRequest) -> CalendarBatch:
        sessions = []
        day = request.start
        while day <= request.end:
            sessions.append(
                Session(
                    market=request.market,
                    trading_date=day,
                    timezone="Asia/Shanghai",
                    phases=(
                        TradingPhase(
                            name="main",
                            start=datetime.combine(day, time(1, 30, tzinfo=timezone.utc)),
                            end=datetime.combine(day, time(7, 0, tzinfo=timezone.utc)),
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
            schema_version="1.0",
            correlation_id=None,
            records=tuple(sessions),
            complete=True,
            next_cursor=None,
            provenance=DataProvenance(
                adapter_name="example-calendar",
                source_revision="static",
                request_fingerprint="example",
                read_at=datetime.now(timezone.utc),
            ),
            quality=QualityReport(status="ok"),
        )
