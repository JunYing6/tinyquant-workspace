"""Smoke tests for the user-side project: adapters, strategy and gateway."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from engines.fast import FastBacktestEngine
from trading_nodes.strategies.simple_strategies import DualMaStrategy
from tools.data import DataRequest

REAL_VOLUME = Path("E:/ProgramData")


@pytest.mark.skipif(
    not (REAL_VOLUME / "reference.duckdb").is_file(),
    reason="workspace volume not mounted",
)
def test_gateway_assembles_and_runs_backtest() -> None:
    """The real-gateway smoke path: data.adapters.build_gateway over the USB volume."""
    from data.adapters import build_gateway

    gateway = build_gateway()
    strategy = DualMaStrategy(["000001.SZ", "600000.SH"])
    engine = FastBacktestEngine(
        strategy,
        "20240102",
        "20240103",
        data_gateway=gateway,
        mode="fast",
        progress_bar=False,
    )
    engine.run()
    stats = engine.get_stats()
    assert stats
    assert stats["final_equity"] > 0


def test_selection_factors_accept_gateway_bar_records() -> None:
    from datetime import timezone

    from tools.data import Bar
    from tools.data import DataRequest
    from tools.data.memory import InMemoryGateway
    from importlib import import_module

    PriceAboveMaSelectionFactor = import_module(
        "trading_nodes.factors.selector.and.price_above_ma"
    ).PriceAboveMaSelectionFactor
    MomentumSelectionFactor = import_module(
        "trading_nodes.factors.selector.float.momentum"
    ).MomentumSelectionFactor

    close = datetime(2024, 1, 2, 7, 0, tzinfo=timezone.utc)
    bar = Bar(
        schema_version="1",
        event_id=None,
        instrument_id="000001.SZ",
        asset_type="equity",
        effective_time=close,
        event_time=close,
        available_at=close,
        trading_date=date(2024, 1, 2),
        source="smoke",
        quality="valid",
        metadata={},
        frequency="1d",
        interval_start=close.replace(hour=1, minute=30),
        interval_end=close,
        open=10.0,
        high=10.0,
        low=10.0,
        close=10.0,
        volume=0.0,
        turnover=0.0,
        is_complete=True,
        price_basis="raw",
    )
    gateway = InMemoryGateway(bars=[bar])
    request = DataRequest(
        dataset="market.bar",
        anchor=date(2024, 1, 2),
        fields=("close",),
        frequency="1d",
    )
    batch = gateway.read(request)

    price_factor = PriceAboveMaSelectionFactor(window=1)
    price_factor._begin_request_generation()
    price_factor.sign["fit"] = True
    price_factor._register_request("data")
    price_factor.receive_data({"request_id": "data"}, batch.records)
    price_factor.sign["data"] = True
    assert price_factor._calculate_internal({"data": batch.records}).index.tolist() == ["000001.SZ"]

    momentum_factor = MomentumSelectionFactor(window=1)
    momentum_factor._begin_request_generation()
    momentum_factor.sign["fit"] = True
    momentum_factor._register_request("data")
    momentum_factor.receive_data({"request_id": "data"}, batch.records)
    momentum_factor.sign["data"] = True
    assert momentum_factor._calculate_internal({"data": batch.records}).empty
