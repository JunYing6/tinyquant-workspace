"""``market.daily_snapshot`` composite adapter.

Reads the canonical ``market.bar`` and ``market.daily_metric`` datasets and
joins them on ``(instrument_id, trading_date)``, returning the manifest-defined
read-only view.  The composition is explicit here (never a silent cross-source
merge inside the gateway).
"""

from __future__ import annotations

import dataclasses
from typing import Any

from tools.data import (
    AdapterDescriptor,
    DataBatch,
    DataProvenance,
    DataRequest,
    DatasetCapability,
    QualityReport,
    default_catalog,
)

ADAPTER_NAME = "workspace-snapshot"
SCHEMA_VERSION = "1"


def _capability() -> DatasetCapability:
    definition = default_catalog().get("market.daily_snapshot")
    return DatasetCapability(
        dataset="market.daily_snapshot",
        modes=("historical",),
        asset_types=frozenset({"equity"}),
        frequencies=(),
        fields=tuple(definition.fields),
        point_in_time=definition.point_in_time,
    )


class DailySnapshotCompositeAdapter:
    """Serves ``market.daily_snapshot`` from market.bar + market.daily_metric."""

    DATASETS = ("market.daily_snapshot",)

    def __init__(self, bars: Any, tables: Any) -> None:
        self._bars = bars
        self._tables = tables
        self.descriptor = AdapterDescriptor(
            name=ADAPTER_NAME,
            datasets={"market.daily_snapshot": _capability()},
            historical_modes=("historical",),
            realtime_modes=(),
            supports_point_in_time=True,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset({"equity"}),
            schema_versions=(SCHEMA_VERSION,),
        )

    def read(self, request: DataRequest) -> DataBatch:
        bar_request = dataclasses.replace(request, dataset="market.bar")
        metric_request = dataclasses.replace(request, dataset="market.daily_metric")
        bar_batch = self._bars.read(bar_request)
        metric_batch = self._tables.read(metric_request)

        metrics: dict[tuple[str, Any], dict[str, Any]] = {}
        for row in metric_batch.records:
            metrics[(row["instrument_id"], row["trading_date"])] = row

        records: list[dict[str, Any]] = []
        for bar in bar_batch.records:
            if bar.asset_type != "equity":
                continue
            key = (bar.instrument_id, bar.trading_date)
            metric = metrics.get(key, {})
            row: dict[str, Any] = {
                "instrument_id": bar.instrument_id,
                "trading_date": bar.trading_date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "turnover": bar.turnover,
                "available_at": max(
                    bar.available_at,
                    metric.get("available_at") or bar.available_at,
                ),
                "effective_time": bar.event_time,
            }
            for name, value in metric.items():
                if name not in row:
                    row[name] = value
            records.append(_project(row, request.fields))
        revision = f"{bar_batch.provenance.source_revision}+{metric_batch.provenance.source_revision}"
        return DataBatch(
            request_id=request.correlation_id or "",
            dataset="market.daily_snapshot",
            schema_version=SCHEMA_VERSION,
            correlation_id=request.correlation_id,
            records=tuple(records),
            complete=True,
            next_cursor=None,
            provenance=DataProvenance(
                adapter_name=ADAPTER_NAME,
                source_revision=revision,
                request_fingerprint=request.delivery_key or "workspace",
                read_at=bar_batch.provenance.read_at,
                upstream_request="market.bar + market.daily_metric composite",
            ),
            quality=QualityReport(status="ok", checked_count=len(records)),
        )

    def iter(self, request: DataRequest, chunk_size: int = 10_000):
        yield self.read(request)


def _project(record: dict[str, Any], fields: tuple[str, ...] | None) -> dict[str, Any]:
    if not fields:
        return record
    return {key: record.get(key) for key in fields if key in record}
