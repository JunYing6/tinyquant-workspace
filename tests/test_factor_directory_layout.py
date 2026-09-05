from __future__ import annotations

from pathlib import Path
from importlib import import_module

PriceAboveMaSelectionFactor = import_module("trading_nodes.factors.selector.and.price_above_ma").PriceAboveMaSelectionFactor
MomentumSelectionFactor = import_module("trading_nodes.factors.selector.float.momentum").MomentumSelectionFactor
AtrStopRiskFactor = import_module("trading_nodes.factors.risk.kline.atr_stop").AtrStopRiskFactor
from trading_nodes.factors.timer.kline.simple_timing import DualMaTimingFactor
from trading_nodes.factors.timer.tick.intent_executor import IntentExecutorFactor
from trading_nodes.methods.selector.fixed import FixedStockPicking
from trading_nodes.minds.weighting import KellyPositionMind, MarketRegimeMind
from trading_nodes.strategies.simple_strategies import EmptyPositionStrategy
from trading_nodes.streams.multi_strategy import MultiStrategyStream


def test_factor_directories_follow_node_stage_layout() -> None:
    root = Path(__file__).parents[1] / "trading_nodes" / "factors"
    expected = [
        root / "selector" / "and",
        root / "selector" / "or",
        root / "selector" / "float",
        root / "timer" / "kline",
        root / "timer" / "tick",
        root / "risk" / "event",
        root / "risk" / "float",
        root / "risk" / "kline",
        root / "risk" / "tick",
    ]
    assert all(path.is_dir() for path in expected)
    assert not (root / "kline").exists()


def test_canonical_nodes_import_and_construct() -> None:
    assert PriceAboveMaSelectionFactor.factor_kind == "selection_binary"
    assert MomentumSelectionFactor.factor_kind == "selection_float"
    assert DualMaTimingFactor.factor_kind == "timing_kline"
    assert IntentExecutorFactor.factor_kind == "timing_tick"
    assert AtrStopRiskFactor.factor_kind == "risk_kline"
    assert isinstance(FixedStockPicking(), FixedStockPicking)
    assert isinstance(EmptyPositionStrategy().selector, FixedStockPicking)
    assert isinstance(KellyPositionMind(), KellyPositionMind)
    assert isinstance(MarketRegimeMind({"down": [], "sideways": [], "up": []}), MarketRegimeMind)
    assert len(MultiStrategyStream().strategies) == 9
