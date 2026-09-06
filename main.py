"""Assemble a user-side DataGateway and run a backtest.

This is the recommended entry point for a user project:

1. put your strategies under ``src/strategies/``,
2. implement your data adapters under ``adapters/``,
3. bind them into a ``DataGateway`` here and feed it to the engine.

Run:

    D:\\Apps\\Python312\\python.exe main.py
"""

from __future__ import annotations

from config import release_settings, settings
from data.adapters.workspace_data import build_workspace_gateway
from engines.fast import FastBacktestEngine
from trading_nodes.strategies.buy_close import BuyCloseStrategy
from tools.excel_report import export_backtest_excel


def build_gateway():
    """Bind the workspace adapters (real E:/ProgramData volume) into a gateway.

    ``build_workspace_gateway(settings.DATA_ROOT)`` serves calendar, bars,
    daily metrics, instruments, industry membership and the per-day tick
    volumes from the consolidated duckdb + parquet layout declared in
    ``data/adapters/workspace_data/mappings.py``.  Swap this builder for your
    own adapter assembly and the strategy + engine stay unchanged.
    """
    return build_workspace_gateway(settings.DATA_ROOT)


def build_backtest():
    return BuyCloseStrategy(list(settings.STOCK_POOL)), build_gateway()


def main() -> int:
    strategy, gateway = build_backtest()
    engine = FastBacktestEngine(
        strategy,
        settings.BACKTEST_START,
        settings.BACKTEST_END,
        initial_capital=settings.INITIAL_CAPITAL,
        data_gateway=gateway,
        mode="fast",
        progress_bar=False,
    )
    engine.run()
    stats = engine.get_stats()
    for key, value in sorted(stats.items()):
        print(f"{key}: {value}")
    report_path = export_backtest_excel(engine, output_dir=release_settings.EXCEL_OUTPUT_DIR)
    print(f"Excel report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
