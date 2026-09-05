"""Smoke tests for the user-side project: adapters, strategy and gateway."""

from __future__ import annotations

from datetime import date, datetime

from adapters.memory_adapters import InMemoryCalendarAdapter, InMemoryHistoricalAdapter
from engines.fast import FastBacktestEngine
from main import SAMPLE_DAILY, build_gateway
from trading_nodes.strategies.buy_close import BuyCloseStrategy
from tools.data import CalendarRequest, DataRequest


def test_strategy_prepares_canonical_request() -> None:
    strategy = BuyCloseStrategy(["000001.SZ"])
    requests = strategy.prepare_requirements(datetime(2024, 1, 2))
    assert len(requests) == 1
    assert requests[0].dataset == "market.bar"
    assert requests[0].frequency == "1d"
    assert requests[0].instruments == ("000001.SZ",)


def test_historical_adapter_serves_bars() -> None:
    adapter = InMemoryHistoricalAdapter(SAMPLE_DAILY)
    request = DataRequest(dataset="market.bar", anchor=date(2024, 1, 2), frequency="1d")
    batch = adapter.read(request)
    assert batch.dataset == "market.bar"
    assert batch.records
    assert all(hasattr(record, "instrument_id") for record in batch.records)


def test_calendar_adapter_serves_sessions() -> None:
    adapter = InMemoryCalendarAdapter()
    batch = adapter.sessions(
        CalendarRequest(market="CN", start=date(2024, 1, 2), end=date(2024, 1, 3))
    )
    assert batch.dataset == "calendar.session"
    assert len(batch.records) == 2


def test_gateway_assembles_and_runs_backtest() -> None:
    gateway = build_gateway()
    strategy = BuyCloseStrategy(["000001.SZ", "600000.SH"])
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
    from tools.data import DataRequest
    from tools.data.memory import InMemoryGateway
    from importlib import import_module

    PriceAboveMaSelectionFactor = import_module(
        "trading_nodes.factors.selector.and.price_above_ma"
    ).PriceAboveMaSelectionFactor
    MomentumSelectionFactor = import_module(
        "trading_nodes.factors.selector.float.momentum"
    ).MomentumSelectionFactor

    gateway = InMemoryGateway({"20240102": [{"code": "000001.SZ", "close": 10.0}]})
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
