from __future__ import annotations

from datetime import datetime, timezone

import pytest

import pandas as pd  # type: ignore[import-untyped]

from trading_nodes_base.types import KlineBar
from trading_nodes_base.methods import BaseRiskControl, BaseStockPicking, BaseTimeSelection, RiskDecision
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes.methods.selector.fixed import FixedStockPicking
from trading_nodes.methods.timer.passive import NoTradeTiming
from trading_nodes.methods.risk.full_position import FullPositionRisk
from trading_nodes.minds.weighting import KellyPositionMind, MarketRegimeMind, PerformanceWeightMind
from tools.data import Bar, InMemoryGateway, Session, TradingPhase
from trading_nodes.strategies.simple_strategies import (
    AtrStopStrategy,
    BreakoutRiskStrategy,
    BreakoutStrategy,
    DualMaStrategy,
    EmptyPositionStrategy,
    FilterPickStrategy,
    GoldenCrossStrategy,
    MeanReversionStrategy,
    MomentumPickStrategy,
)
from trading_nodes.streams.multi_strategy import MultiStrategyStream
from engines.fast import FastBacktestEngine


def sample_bar(close: float = 10.0) -> KlineBar:
    return KlineBar("000001.SZ", "1d", datetime(2024, 1, 2), close, close, close, close, 100, close * 100)


def test_fixed_stock_picking_defaults_empty_and_preserves_explicit_pool() -> None:
    empty = FixedStockPicking()
    assert list(empty.select_stocks("20240102").columns) == ["asset", "weight"]
    assert empty.select_stocks("20240102").empty
    picker = FixedStockPicking(["000001.SZ", "000001.SZ", "600000.SH"])
    result = picker.select_stocks("20240102")
    assert result["asset"].tolist() == ["000001.SZ", "600000.SH"]
    assert result["weight"].tolist() == [0.5, 0.5]


def test_empty_position_components_are_non_trading() -> None:
    strategy = EmptyPositionStrategy()
    assert isinstance(strategy.selector, FixedStockPicking)
    assert strategy.selector.stock_pool == []
    assert isinstance(strategy.timer, NoTradeTiming)
    assert isinstance(strategy.risk_ctrl, FullPositionRisk)
    assert strategy.timer.kline_bar_input(sample_bar()) == []
    assert strategy.risk_ctrl.on_daily().target_position_ratio == 1.0


def test_all_nine_strategies_use_component_instances() -> None:
    strategy_types = [
        DualMaStrategy,
        BreakoutStrategy,
        MeanReversionStrategy,
        GoldenCrossStrategy,
        AtrStopStrategy,
        BreakoutRiskStrategy,
        MomentumPickStrategy,
        FilterPickStrategy,
        EmptyPositionStrategy,
    ]
    for strategy_type in strategy_types:
        strategy = strategy_type()
        assert isinstance(strategy, BaseStrategy)
        assert isinstance(strategy.selector, BaseStockPicking)
        assert isinstance(strategy.timer, BaseTimeSelection)
        assert isinstance(strategy.risk_ctrl, BaseRiskControl)
        assert strategy.supports_fast_backtest is True
        assert strategy.timer.fast_eligible is True


def test_performance_weight_mind_favors_recently_stronger_strategy() -> None:
    mind = PerformanceWeightMind(lookback=3)
    for equity in [100, 102, 105, 110]:
        mind.update_performance("good", {"total_equity": equity})
    for equity in [100, 101, 100, 99]:
        mind.update_performance("bad", {"total_equity": equity})
    weights = mind.calculate_weights({}, {"good": {}, "bad": {}})
    assert weights["good"] > weights["bad"]
    assert abs(sum(weights.values()) - 1.0) < 1e-9


def test_multi_strategy_stream_contains_nine_children_and_one_mind() -> None:
    stream = MultiStrategyStream()
    assert len(stream.strategies) == 9
    assert isinstance(stream.mind, PerformanceWeightMind)


def test_kelly_mind_caps_excess_risk_and_assigns_remainder_to_empty_strategy() -> None:
    mind = KellyPositionMind(empty_strategy_name="empty-position", fraction=0.5)
    stats = {
        "trend": {"win_rate": 0.6, "avg_win": 0.08, "avg_loss": 0.04},
        "empty-position": {"win_rate": 0.0, "avg_win": 0.0, "avg_loss": 0.0},
    }
    weights = mind.calculate_weights({}, stats)
    assert weights["trend"] == pytest.approx(0.2)
    assert weights["empty-position"] == pytest.approx(0.8)
    assert sum(weights.values()) == 1.0


def test_regime_mind_routes_downside_sideways_and_uptrend() -> None:
    mind = MarketRegimeMind(
        regime_strategies={
            "down": ["empty-position"],
            "sideways": ["mean-reversion"],
            "up": ["momentum-pick"],
        }
    )
    assert mind.calculate_weights({"regime": "down"}, {"empty-position": {}, "momentum-pick": {}, "mean-reversion": {}}) == {"empty-position": 1.0, "momentum-pick": 0.0, "mean-reversion": 0.0}
    assert mind.calculate_weights({"regime": "sideways"}, {"empty-position": {}, "momentum-pick": {}, "mean-reversion": {}})["mean-reversion"] == 1.0
    assert mind.calculate_weights({"regime": "up"}, {"empty-position": {}, "momentum-pick": {}, "mean-reversion": {}})["momentum-pick"] == 1.0


def _two_day_gateway() -> InMemoryGateway:
    """Minimal hermetic fixture: two sessions, one bar per day."""
    bars, sessions = [], []
    for day in ("20240102", "20240103"):
        close = datetime.strptime(day, "%Y%m%d").replace(hour=15, tzinfo=timezone.utc)
        phase = TradingPhase(name="regular", start=close.replace(hour=9), end=close, accepts_trades=True, accepts_quotes=True)
        sessions.append(Session(market="CN", trading_date=close.date(), timezone="UTC", phases=(phase,)))
        bars.append(Bar(
            schema_version="1", event_id=None, instrument_id="000001.SZ", asset_type="equity",
            effective_time=close, event_time=close, available_at=close, trading_date=close.date(),
            source="test", quality="valid", metadata={}, frequency="1d",
            interval_start=close.replace(hour=9), interval_end=close,
            open=10.0, high=11.0, low=9.5, close=10.5, volume=1000.0, turnover=10500.0,
            is_complete=True, price_basis="raw",
        ))
    return InMemoryGateway(bars=bars, sessions=sessions)


def test_empty_position_strategy_stays_empty_in_fast_backtest() -> None:
    strategy = EmptyPositionStrategy()
    engine = FastBacktestEngine(
        strategy,
        "20240102",
        "20240103",
        mode="fast",
        data_gateway=_two_day_gateway(),
        progress_bar=False,
    )
    engine.run()
    assert engine.account.positions == {}
    assert engine.account.trade_log == {}
