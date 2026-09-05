from __future__ import annotations

from data.adapters.strategy_data import InMemoryStrategyData
from engines.fast import FastBacktestEngine
from trading_nodes.strategies.simple_strategies import (
    AtrStopStrategy, BreakoutRiskStrategy, BreakoutStrategy, DualMaStrategy,
    EmptyPositionStrategy, FilterPickStrategy, GoldenCrossStrategy,
    MeanReversionStrategy, MomentumPickStrategy,
)
from trading_nodes.streams.multi_strategy import MultiStrategyStream


def main() -> int:
    data = InMemoryStrategyData()
    end = data.dates_in_range("20240102", "99991231")[-1]
    strategies = [DualMaStrategy, BreakoutStrategy, MeanReversionStrategy, GoldenCrossStrategy, AtrStopStrategy, BreakoutRiskStrategy, MomentumPickStrategy, FilterPickStrategy, EmptyPositionStrategy]
    for strategy_type in strategies:
        engine = FastBacktestEngine(strategy_type(), "20240102", end, data_gateway=data, mode="fast", progress_bar=False)
        engine.run()
        stats = engine.get_stats()
        print(f"{strategy_type.__name__}: final_equity={stats['final_equity']:.2f} trades={stats.get('trade_count', 0)}")
    stream = MultiStrategyStream()
    engine = FastBacktestEngine(stream, "20240102", end, data_gateway=data, mode="fast", progress_bar=False)
    engine.run()
    print(f"stream weights: {stream.mind.current_weights}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
