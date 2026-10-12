"""Smoke tests for the user-side project: adapters, strategy and gateway."""

from __future__ import annotations

from pathlib import Path

import pytest

from engines.fast import FastBacktestEngine
from trading_nodes.strategies.simple_strategies import DualMaStrategy

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

