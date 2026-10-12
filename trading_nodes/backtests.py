from __future__ import annotations

from config import settings
from data.adapters import build_gateway
from trading_nodes.strategies.kdj_volume_strategy import KdjVolumeStrategy
from trading_nodes.strategies.magic_formula import MagicFormulaStrategy
from trading_nodes.strategies.simple_strategies import (
    AtrStopStrategy,
    BreakoutRiskStrategy,
    BreakoutStrategy,
    DualMaStrategy,
    EmptyPositionStrategy,
    GoldenCrossStrategy,
    MeanReversionStrategy,
)
from trading_nodes.streams.multi_strategy import MultiStrategyStream

def build_magic_formula():
    return MagicFormulaStrategy(list(settings.STOCK_POOL), top_n=5), build_gateway()


def build_kdj_volume():
    return KdjVolumeStrategy(list(settings.STOCK_POOL)), build_gateway()


def build_dual_ma():
    return DualMaStrategy(list(settings.STOCK_POOL)), build_gateway()


def build_breakout():
    return BreakoutStrategy(list(settings.STOCK_POOL)), build_gateway()


def build_mean_reversion():
    return MeanReversionStrategy(list(settings.STOCK_POOL)), build_gateway()


def build_golden_cross():
    return GoldenCrossStrategy(list(settings.STOCK_POOL)), build_gateway()


def build_atr_stop():
    return AtrStopStrategy(list(settings.STOCK_POOL)), build_gateway()


def build_breakout_risk():
    return BreakoutRiskStrategy(list(settings.STOCK_POOL)), build_gateway()


def build_empty_position():
    return EmptyPositionStrategy(list(settings.STOCK_POOL)), build_gateway()


def build_stream():
    return MultiStrategyStream(), build_gateway()


BACKTESTS = [
    {"name": "神奇公式(magic-formula)", "kind": "strategy", "factory": "trading_nodes.backtests:build_magic_formula"},
    {"name": "KDJ放量(kdj-volume)", "kind": "strategy", "factory": "trading_nodes.backtests:build_kdj_volume"},
    {"name": "双均线(dual-ma)", "kind": "strategy", "factory": "trading_nodes.backtests:build_dual_ma"},
    {"name": "突破(breakout)", "kind": "strategy", "factory": "trading_nodes.backtests:build_breakout"},
    {"name": "均值回归(mean-reversion)", "kind": "strategy", "factory": "trading_nodes.backtests:build_mean_reversion"},
    {"name": "金叉(golden-cross)", "kind": "strategy", "factory": "trading_nodes.backtests:build_golden_cross"},
    {"name": "ATR止损(atr-stop)", "kind": "strategy", "factory": "trading_nodes.backtests:build_atr_stop"},
    {"name": "突破风控(breakout-risk)", "kind": "strategy", "factory": "trading_nodes.backtests:build_breakout_risk"},
    {"name": "空仓(empty-position)", "kind": "strategy", "factory": "trading_nodes.backtests:build_empty_position"},
    {"name": "七策略组合(multi-strategy)", "kind": "stream", "factory": "trading_nodes.backtests:build_stream"},
]
