from engines.fast import FastBacktestEngine
from main import build_backtest
from trading_nodes_base.strategies import BaseStrategy
from tools.data import DataGateway


def test_build_backtest_returns_entity_and_gateway() -> None:
    entity, gateway = build_backtest()
    assert isinstance(entity, BaseStrategy)
    assert isinstance(gateway, DataGateway)


def test_factory_can_construct_gateway_engine() -> None:
    entity, gateway = build_backtest()
    engine = FastBacktestEngine(entity, "20240102", "20240103", data_gateway=gateway, mode="fast", progress_bar=False)
    engine.run()
    assert engine.get_stats()["final_equity"] > 0
