"""``derived.technical_indicator`` —— 基于标准 kline 的指标计算。

按照数据扩展设计（第 5.10 节）的第二阶段处理：衍生适配器读取*标准*的
``market.bar`` 数据集，计算指标，并返回一个标准 ``TableBatch``，其溯源携带
输入数据版本与计算版本。它绝不绕过网关去触碰原始厂商表。

指标名内嵌其参数，例如 ``filters={"indicator": "ma20"}``（最近 20 个收盘价
的均值）或 ``"ema12"``。每行的参数哈希作为 (instrument, date, indicator,
params) 元组的键，用于缓存/去重。
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
    """从标准 ``market.bar`` 计算 ``derived.technical_indicator``。"""

    DATASETS = (DATASET,)

    def __init__(self, bars: Any) -> None:
        """``bars`` 必须提供 ``market.bar`` 数据（例如 ``WorkspaceBarAdapter``）。"""
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
