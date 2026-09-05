"""``derived.technical_indicator`` — indicator computation over canonical bars.

Second-stage processing per the data-extension design (section 5.10): the
derived adapter reads the *canonical* ``market.bar`` dataset, computes an
indicator, and returns a standard ``TableBatch`` whose provenance carries the
input data revision and the calculation version.  It never bypasses the
gateway to touch raw vendor tables.

Indicator names embed their parameter, e.g. ``filters={"indicator": "ma20"}``
(mean of the last 20 closes) or ``"ema12"``.  The parameter hash in each row
keys the (instrument, date, indicator, params) tuple for caching/dedup.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pandas as pd
from tools.data import (
    AdapterDescriptor,
    DataBatch,
    DataProvenance,
    DataRequest,
    DatasetCapability,
    QualityReport,
    default_catalog,
)

from data.processing.base import calculation_version, parameter_hash

ADAPTER_NAME = "workspace-derived"
SCHEMA_VERSION = "1"
CALC_VERSION = calculation_version("data.processing.indicators", "1.0.0")
DATASET = "derived.technical_indicator"


def _capability() -> DatasetCapability:
    definition = default_catalog().get(DATASET)
    return DatasetCapability(
        dataset=DATASET,
        modes=("historical",),
        asset_types=frozenset(definition.asset_types),
        frequencies=(),
        fields=tuple(definition.fields),
        point_in_time=definition.point_in_time,
    )


class DerivedIndicatorAdapter:
    """Computes ``derived.technical_indicator`` from canonical ``market.bar``."""

    DATASETS = (DATASET,)

    def __init__(self, bars: Any) -> None:
        """``bars`` must serve ``market.bar`` (e.g. ``WorkspaceBarAdapter``)."""
        self._bars = bars
        self.descriptor = AdapterDescriptor(
            name=ADAPTER_NAME,
            datasets={DATASET: _capability()},
            historical_modes=("historical",),
            realtime_modes=(),
            supports_point_in_time=True,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset({"equity", "fund", "index"}),
            schema_versions=(SCHEMA_VERSION,),
        )

    def read(self, request: DataRequest) -> DataBatch:
        indicator = str(request.filters.get("indicator") or "")
        name, period = _parse_indicator(indicator)
        bar_request = dataclasses.replace(
            request,
            dataset="market.bar",
            fields=None,
            filters={},
        )
        bar_batch = self._bars.read(bar_request)

        frame = pd.DataFrame(
            [
                {
                    "instrument_id": bar.instrument_id,
                    "trading_date": bar.trading_date,
                    "close": bar.close,
                    "available_at": bar.available_at,
                    "effective_time": bar.event_time,
                    "asset_type": bar.asset_type,
                }
                for bar in bar_batch.records
            ]
        )
        records: list[dict[str, Any]] = []
        if not frame.empty:
            frame = frame.sort_values(["instrument_id", "trading_date"])
            param_hash = parameter_hash(name, period)
            for code, group in frame.groupby("instrument_id"):
                series = group["close"]
                if name == "ema":
                    values = series.ewm(span=period, adjust=False).mean()
                else:  # "ma"
                    values = series.rolling(window=period, min_periods=period).mean()
                for (_, row), value in zip(group.iterrows(), values):
                    if pd.isna(value):
                        continue
                    records.append(
                        {
                            "instrument_id": row["instrument_id"],
                            "trading_date": row["trading_date"],
                            "indicator": indicator,
                            "value": float(value),
                            "parameter_hash": param_hash,
                            "asset_type": row["asset_type"],
                            "available_at": row["available_at"],
                            "effective_time": row["effective_time"],
                            "source": ADAPTER_NAME,
                            "quality": "valid",
                            "metadata": {
                                "period": period,
                                "input_revision": bar_batch.provenance.source_revision,
                                "calc_version": CALC_VERSION,
                            },
                        }
                    )
        records.sort(key=lambda r: (r["instrument_id"], r["trading_date"]))
        requested = request.fields
        if requested:
            records = [{k: v for k, v in row.items() if k in requested} for row in records]
        return DataBatch(
            request_id=request.correlation_id or "",
            dataset=DATASET,
            schema_version=SCHEMA_VERSION,
            correlation_id=request.correlation_id,
            records=tuple(records),
            complete=True,
            next_cursor=None,
            provenance=DataProvenance(
                adapter_name=ADAPTER_NAME,
                source_revision=bar_batch.provenance.source_revision,
                request_fingerprint=request.delivery_key or "workspace",
                read_at=bar_batch.provenance.read_at,
                upstream_request=f"market.bar + {indicator} ({CALC_VERSION})",
            ),
            quality=QualityReport(status="ok", checked_count=len(records)),
        )

    def iter(self, request: DataRequest, chunk_size: int = 10_000):
        yield self.read(request)


def _parse_indicator(indicator: str) -> tuple[str, int]:
    name = indicator.rstrip("0123456789")
    digits = indicator[len(name):]
    if not name or not digits:
        raise ValueError(
            f"indicator must embed its period, e.g. 'ma20' or 'ema12' (got {indicator!r})"
        )
    period = int(digits)
    if period <= 0:
        raise ValueError(f"indicator period must be positive (got {indicator!r})")
    return name, period


__all__ = ["DerivedIndicatorAdapter"]
