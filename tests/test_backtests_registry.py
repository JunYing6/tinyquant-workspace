from __future__ import annotations

from importlib import import_module

from trading_nodes.backtests import BACKTESTS
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.streams import BaseStream


def test_backtests_include_all_expected_items() -> None:
    names = {entry["name"] for entry in BACKTESTS}
    assert "买入持有(buy-close)" in names
    assert "双均线(dual-ma)" in names
    assert "九策略组合(multi-strategy)" in names
    assert any(entry["kind"] == "stream" for entry in BACKTESTS)
    assert any(entry["kind"] == "strategy" for entry in BACKTESTS)


def test_every_factory_returns_entity_and_gateway() -> None:
    def _load(factory: str):
        module_name, _, func_name = factory.partition(":")
        return getattr(import_module(module_name), func_name)()

    for entry in BACKTESTS:
        entity, gateway = _load(entry["factory"])
        assert isinstance(entity, (BaseStrategy, BaseStream))
        assert callable(getattr(gateway, "read", None))
        assert callable(getattr(gateway, "sessions", None))
