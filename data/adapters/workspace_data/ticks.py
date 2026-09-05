"""Per-day tick adapter: ``market.trade`` and ``market.quote``.

Physical source: ``{root}/{year}/{MMDD}/stock.parquet`` (equities, five-level
book) and ``{root}/{year}/{MMDD}/index.parquet`` (index trades).  One row may
carry both a trade and a book state: rows with ``pr > 0`` emit a ``TradeTick``
and rows with any valid book level emit a ``QuoteTick`` (the design allows both
from one source row when both facts are proven; ``pr == 0`` rows are
quote-only snapshots and never fabricate a trade).

Sequences are synthetic (row order per instrument per day) and reported as a
``SEQUENCE_SYNTHETIC`` quality warning; strict cross-instrument replay must
therefore not rely on global ordering (the source declares per-instrument
ordering only).
"""

from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import pandas as pd
from tools.data import (
    AdapterDescriptor,
    DataBatch,
    DataProvenance,
    DataRequest,
    DatasetCapability,
    PriceLevel,
    QualityReport,
    QualityWarning,
    TradeTick,
    QuoteTick,
    default_catalog,
)

from data.adapters.workspace_data.reader import CST, date_range_days, source_revision

ADAPTER_NAME = "workspace-tick"
SCHEMA_VERSION = "1"
SOURCE = "workspace-tick"

_EQUITY_SIZE_FACTOR = 100.0  # lots -> shares for equities
_INDEX_SIZE_FACTOR = 1.0  # index quantity keeps the source-native unit

_SIDE_MAP = {"B": "BUY", "S": "SELL"}
_LEVEL_COLUMNS = tuple((f"b{i}p", f"b{i}v") for i in range(1, 6))
_ASK_LEVEL_COLUMNS = tuple((f"s{i}p", f"s{i}v") for i in range(1, 6))


def _capability(dataset: str) -> DatasetCapability:
    definition = default_catalog().get(dataset)
    return DatasetCapability(
        dataset=dataset,
        modes=("historical",),
        asset_types=frozenset({"equity", "index"}),
        frequencies=(),
        fields=tuple(definition.fields),
        point_in_time=definition.point_in_time,
    )


class WorkspaceTickAdapter:
    """Serves ``market.trade`` / ``market.quote`` from the per-day tick files."""

    DATASETS = ("market.trade", "market.quote")

    def __init__(self, data_root: str | Path, market: str = "CN") -> None:
        self._root = Path(data_root)
        self.descriptor = AdapterDescriptor(
            name=ADAPTER_NAME,
            datasets={
                "market.trade": _capability("market.trade"),
                "market.quote": _capability("market.quote"),
            },
            historical_modes=("historical",),
            realtime_modes=(),
            supports_point_in_time=True,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset({"equity", "index"}),
            schema_versions=(SCHEMA_VERSION,),
            ordering_guarantee="per_instrument",
        )

    def read(self, request: DataRequest) -> DataBatch:
        if request.dataset == "market.trade":
            return self._read(request, want_trades=True, want_quotes=False)
        if request.dataset == "market.quote":
            return self._read(request, want_trades=False, want_quotes=True)
        raise ValueError(f"workspace-tick adapter does not serve {request.dataset!r}")

    def iter(self, request: DataRequest, chunk_size: int = 10_000):
        yield self.read(request)

    def _day_file(self, day: date, asset_type: str) -> Path:
        sub = day.strftime("%m%d")
        name = "stock.parquet" if asset_type == "equity" else "index.parquet"
        return self._root / str(day.year) / sub / name

    def _read(self, request: DataRequest, *, want_trades: bool, want_quotes: bool) -> DataBatch:
        if request.start is None or request.end is None:
            raise ValueError("market.trade/quote requests require start and end (session bounds)")
        days = date_range_days(request.start, request.end)
        asset_filter = request.asset_type if request.asset_type in ("equity", "index") else None
        asset_types = (asset_filter,) if asset_filter else ("equity", "index")

        records: list[Any] = []
        used_files: list[Path] = []
        for asset_type in asset_types:
            for day in days:
                path = self._day_file(day, asset_type)
                if not path.is_file():
                    continue
                used_files.append(path)
                frame = self._read_day(path, day, request, asset_type)
                factor = _EQUITY_SIZE_FACTOR if asset_type == "equity" else _INDEX_SIZE_FACTOR
                counters: dict[str, int] = {}
                for row in frame.to_dict("records"):
                    code = str(row.get("code"))
                    event_time = self._event_time(day, str(row.get("time")))
                    if request.as_of is not None and event_time > request.as_of:
                        continue
                    counters[code] = counters.get(code, -1) + 1
                    sequence = counters[code]
                    price = float(row.get("pr") or 0.0)
                    bid_levels, ask_levels = self._levels(row, asset_type)
                    if want_quotes and (bid_levels or ask_levels):
                        records.append(
                            QuoteTick(
                                schema_version=SCHEMA_VERSION,
                                event_id=None,
                                instrument_id=code,
                                asset_type=asset_type,
                                effective_time=event_time,
                                event_time=event_time,
                                available_at=event_time,
                                trading_date=day,
                                source=SOURCE,
                                quality="valid",
                                metadata={},
                                event_type="quote",
                                bid_levels=tuple(bid_levels),
                                ask_levels=tuple(ask_levels),
                                last_price=price if price > 0 else None,
                                last_size=float(row["vol"]) * factor if float(row.get("vol") or 0.0) > 0 else None,
                                sequence=sequence,
                            )
                        )
                    if want_trades and price > 0:
                        records.append(
                            TradeTick(
                                schema_version=SCHEMA_VERSION,
                                event_id=None,
                                instrument_id=code,
                                asset_type=asset_type,
                                effective_time=event_time,
                                event_time=event_time,
                                available_at=event_time,
                                trading_date=day,
                                source=SOURCE,
                                quality="valid",
                                metadata={},
                                event_type="trade",
                                price=price,
                                size=float(row.get("vol") or 0.0) * factor,
                                turnover=float(row.get("amount") or 0.0),
                                side=_SIDE_MAP.get(str(row.get("bs")), "UNKNOWN"),
                                sequence=sequence,
                            )
                        )
        records.sort(key=lambda r: (r.instrument_id, r.event_time, r.sequence))
        return self._batch(request, records, used_files)

    def _read_day(
        self,
        path: Path,
        day: date,
        request: DataRequest,
        asset_type: str,
    ) -> pd.DataFrame:
        filters: list[Any] = []
        if request.instruments is not None:
            filters.append(("code", "in", [str(code) for code in request.instruments]))
        first_day = day == (request.start.date() if isinstance(request.start, datetime) else request.start)
        last_day = day == (request.end.date() if isinstance(request.end, datetime) else request.end)
        start_t = request.start.time() if isinstance(request.start, datetime) and first_day else time(0, 0, 0)
        end_t = request.end.time() if isinstance(request.end, datetime) and last_day else time(23, 59, 59)
        if start_t > time(0, 0, 0):
            filters.append(("time", ">=", start_t.strftime("%H:%M:%S")))
        if end_t > time(0, 0, 0) and end_t < time(23, 59, 59):
            filters.append(("time", "<", end_t.strftime("%H:%M:%S")))
        try:
            frame = pd.read_parquet(path, filters=filters or None)
        except Exception:
            frame = pd.read_parquet(path)
            if request.instruments is not None:
                frame = frame[frame["code"].isin(request.instruments)]
        return frame

    @staticmethod
    def _event_time(day: date, clock: str) -> datetime:
        parts = [int(piece) for piece in str(clock).split(":")]
        while len(parts) < 3:
            parts.append(0)
        return datetime(day.year, day.month, day.day, parts[0], parts[1], parts[2], tzinfo=CST)

    @staticmethod
    def _levels(row: dict[str, Any], asset_type: str) -> tuple[list[PriceLevel], list[PriceLevel]]:
        factor = _EQUITY_SIZE_FACTOR if asset_type == "equity" else _INDEX_SIZE_FACTOR
        bids: list[PriceLevel] = []
        asks: list[PriceLevel] = []
        for level, (price_col, size_col) in enumerate(_LEVEL_COLUMNS, start=1):
            price = float(row.get(price_col) or 0.0)
            if price > 0:
                bids.append(PriceLevel(price=price, size=float(row.get(size_col) or 0.0) * factor, level=level))
        for level, (price_col, size_col) in enumerate(_ASK_LEVEL_COLUMNS, start=1):
            price = float(row.get(price_col) or 0.0)
            if price > 0:
                asks.append(PriceLevel(price=price, size=float(row.get(size_col) or 0.0) * factor, level=level))
        return bids, asks

    def _batch(self, request: DataRequest, records: list[Any], used_files: list[Path]) -> DataBatch:
        warnings: tuple[QualityWarning, ...] = ()
        status = "ok"
        if records:
            warnings = (
                QualityWarning(
                    code="SEQUENCE_SYNTHETIC",
                    message="source has no sequence column; per-instrument row order used",
                    count=len(records),
                    severity="warning",
                ),
            )
            status = "warning"
        dataset = request.dataset
        revision = source_revision(used_files) if used_files else "wp-empty"
        return DataBatch(
            request_id=request.correlation_id or "",
            dataset=dataset,
            schema_version=SCHEMA_VERSION,
            correlation_id=request.correlation_id,
            records=tuple(records),
            complete=True,
            next_cursor=None,
            provenance=DataProvenance(
                adapter_name=ADAPTER_NAME,
                source_revision=revision,
                request_fingerprint=request.delivery_key or "workspace",
                read_at=datetime.now(tz=CST),
            ),
            quality=QualityReport(status=status, warnings=warnings, checked_count=len(records)),
        )
