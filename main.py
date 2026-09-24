"""tinyquant 工作区的用户侧入口。

不带参数运行 ``python main.py`` 以启动 ``tq`` CLI 工作台（交互式 REPL）；
任何参数都会转发给 CLI。如果 CLI（安装带 ``[cli]`` 附加依赖的
``tinyquant``）不可用，``main.py`` 会回退为直接运行双均线回测。

回测工厂与注册表位于 ``trading_nodes/backtests.py``；数据网关根据
``config/settings.DATA_GATEWAY_FACTORY`` 组装。
"""

from __future__ import annotations

import sys

from config import release_settings, settings
from engines.fast import FastBacktestEngine
from tools.excel_report import export_backtest_excel


def _run_backtest(write_excel: bool, excel_dir: str | None = None) -> int:
    from trading_nodes.backtests import build_dual_ma

    strategy, gateway = build_dual_ma()
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
    if write_excel:
        report_path = export_backtest_excel(engine, output_dir=excel_dir)
        print(f"Excel report: {report_path}")
    return 0


def _launch_cli(argv: list[str]) -> int | None:
    try:
        from tinyquant_cli import app
    except ImportError:
        return None
    return app.main(argv)


def _excel_args(argv: list[str]) -> tuple[bool, str | None]:
    write_excel = "--excel" in argv
    excel_dir = None
    for index, token in enumerate(argv):
        if token == "--excel-dir" and index + 1 < len(argv):
            excel_dir = argv[index + 1]
            write_excel = True
    return write_excel, excel_dir


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    code = _launch_cli(args)
    if code is not None:
        return code
    write_excel, excel_dir = _excel_args(args)
    if excel_dir is None:
        excel_dir = release_settings.EXCEL_OUTPUT_DIR
    return _run_backtest(write_excel=write_excel, excel_dir=excel_dir)


if __name__ == "__main__":
    raise SystemExit(main())
