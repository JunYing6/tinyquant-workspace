from tinyquant_cli.loading import load_backtest_factory
from trading_nodes_base.strategies.base import BaseStrategy
from tools.data import DataGateway


def test_tinyquant_cli_loads_user_factory() -> None:
    entity, gateway = load_backtest_factory("main:build_backtest")

    assert isinstance(entity, BaseStrategy)
    assert isinstance(gateway, DataGateway)
