"""DuckDBStorage：共享上下文持久化（收盘落盘 / 盘前恢复）测试。"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from engines.realtime import RealTimeTradeEngine
from persistence.duckdb_storage import DuckDBStorage
from trading_nodes_base.minds import BaseMind
from trading_nodes_base.streams import BaseStream
from tools.data import InMemoryGateway, Session, TradingPhase


class _EqualMind(BaseMind):
    def calculate_weights(self, market_data, strategies_performance):
        return {name: 1.0 for name in strategies_performance}


def _strategy():
    from trading_nodes_base.factors import IntradayKlineFactor
    from trading_nodes_base.methods import BaseTimeSelection
    from trading_nodes_base.strategies import BaseStrategy

    timer = BaseTimeSelection("t", [IntradayKlineFactor("k")], [])
    return BaseStrategy("child", timer=timer)


def _gateway(day: str = "20240102") -> InMemoryGateway:
    open_at = datetime.strptime(f"{day} 09:30:00", "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)
    close_at = datetime.strptime(f"{day} 15:00:00", "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)
    phase = TradingPhase(name="regular", start=open_at, end=close_at, accepts_trades=True, accepts_quotes=True)
    return InMemoryGateway(sessions=[Session(market="CN", trading_date=close_at.date(), timezone="UTC", phases=(phase,))])


class _MockTradeExecutor:
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def get_account(self) -> dict: return {}
    def get_positions(self) -> list: return []
    def buy(self, *a, **k): return {}
    def sell(self, *a, **k): return {}


def test_duckdb_storage_round_trip(tmp_path) -> None:
    storage = DuckDBStorage(str(tmp_path / "ctx.duckdb"))
    storage.save({"strat:a": {"k": 1, "s": "x"}, "engine:sim_account": {"nested": {"p": 5}}})
    assert storage.load()["strat:a"] == {"k": 1, "s": "x"}
    assert storage.load()["engine:sim_account"]["nested"]["p"] == 5


def test_duckdb_storage_dirty_merge_preserves_others(tmp_path) -> None:
    storage = DuckDBStorage(str(tmp_path / "ctx.duckdb"))
    storage.save({"strat:a": {"k": 1}, "strat:b": {"k": 2}})
    storage.save({"nodes": {"strat:a": {"k": 10}}})  # save_dirty 增量
    loaded = storage.load()
    assert loaded["strat:a"] == {"k": 10}
    assert loaded["strat:b"] == {"k": 2}


def test_realtime_simulate_duckdb_survives_shutdown(tmp_path) -> None:
    db = str(tmp_path / "sim.duckdb")
    child = _strategy()
    stream = BaseStream("sim-stream", [child], _EqualMind())
    engine = RealTimeTradeEngine(
        stream, data_gateway=_gateway(), trade_executor=_MockTradeExecutor(),
        initial_capital=100_000, simulate=True, state_storage=DuckDBStorage(db),
    )
    engine._current_date = datetime.now(timezone.utc).date()
    engine._execute_order(("20240102", "09:31:00", "000001.SZ", 10.0, 100, "n"))
    assert engine.account.positions == {"000001.SZ": 100}
    engine._save_sim_state()  # 收盘落盘（duckdb）

    restored = RealTimeTradeEngine(
        BaseStream("sim-stream", [_strategy()], _EqualMind()),
        data_gateway=_gateway(), trade_executor=_MockTradeExecutor(),
        initial_capital=100_000, simulate=True, state_storage=DuckDBStorage(db),
    )
    restored._restore_sim_state()

    assert restored.account.positions == {"000001.SZ": 100}