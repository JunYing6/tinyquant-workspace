# tinyquant-workspace

用户端策略工作区，用于编写具体量化节点并组装真实数据适配器。框架 `tinyquant` 只发布核心运行时与通用 `tq` CLI，不内置任何具体策略、Mind、Stream 或数据适配器；具体的因子、选股器、择时器、风控、策略与真实数据链路都写在这里。

## 目录

```text
trading_nodes/        具体因子、选股器、择时器、风控、策略、Mind、Stream
data/                 数据域总包（下载 → 适配 → 加工）
├── downloader/       Tushare 数据下载：vendor API -> duckdb 整合卷
│   └── scopes/       30 类数据范围（日线/分钟/财务/资金流/指数/日历等）
├── adapters/
│   └── workspace_data/  真实数据适配器：日历/主数据/行业/K线/日线指标/Tick/Snapshot
└── processing/       二级加工：标准数据集 -> 派生数据集（指标计算）
config/               项目配置（settings.py 与 release_settings.py）
main.py               Gateway 组装、build_backtest 工厂与 CLI 启动入口
excel_reports/        Excel 报告默认输出目录（内容由 .gitignore 忽略）
tests/                用户节点与集成测试
```

具体节点必须从发布版 `trading_nodes_base` 继承基类、使用发布版 `tools.data` 标准数据类型；具体节点之间互相引用使用用户包 `trading_nodes`。

## 安装

框架从源码（含 CLI）安装进当前 Python：

```powershell
D:\Apps\Python312\python.exe -m pip install -e "D:\ProgramFile\tinyquant\tinyquant[cli]"
D:\Apps\Python312\python.exe -m pip install -e ".[dev]"
```

正式使用时把 `pyproject.toml` 的依赖改为发布的 `tinyquant[cli]`，并安装必需的数据依赖（`[workspace-data]`：duckdb、pandas、pyarrow 等）。

## 运行

运行 `python main.py` 会自动启动发布版的 `tq` CLI 工作台（REPL），并把参数原样转发给 CLI：

```powershell
python main.py                                    # 进入 tq 交互式工作台
python main.py help
python main.py backtest run main:build_backtest --start 20240102 --end 20240103
```

`main.py` 内的 `build_gateway` / `build_backtest` 提供真实数据 Gateway 与策略工厂；若未安装 CLI（`tinyquant[cli]`），`python main.py` 会回退为直接运行回测。

`tq` 命令属于已安装的发布版 `tinyquant[cli]`，不需要在本项目内重复实现。

## Excel 回测报告

Excel 报告**默认不导出**，仅在显式指定时生成（策略概览、权益曲线、交易记录、持仓明细、月度收益）：

- CLI：`tq backtest run ... --excel`（默认目录）或 `--excel-dir <路径>`（指定目录）
- `main.py` 回退运行：追加 `--excel` 或 `--excel-dir <路径>`

默认输出目录由 `config/release_settings.py` 的 `EXCEL_OUTPUT_DIR` 配置，默认落在项目根目录的 `excel_reports/`（不存在时自动创建）。

## 数据链路

配置项分散在 `config/`：

- `settings.py` —— 客户端读取的值：回测区间 `BACKTEST_START/END`、初始资金 `INITIAL_CAPITAL`、股票池 `STOCK_POOL`、Tushare `QUOTE_TOKEN`、数据卷根目录 `DATA_ROOT`。
- `release_settings.py` —— 回传给框架组件的值：`EXCEL_OUTPUT_DIR`。

适配器从 `DATA_ROOT`（duckdb 表 + 每日 parquet 的整合卷）读取行情，组装为 `build_workspace_gateway()` 返回的 `tools.data.DataGateway`。Tushare 下载器通过 `data/downloader/tushare.py` 与 `scopes/` 拉取原始数据，`migrate.py` / `tick_import.py` 负责把旧卷或分笔导入整合布局。

## 开发约定

- `trading_nodes/` 只编写具体节点逻辑，通过标准 `DataRequest` 请求数据。
- 真实数据适配器位于 `data/adapters/workspace_data/`，实现 `tools.data` 的历史、实时与交易日历 Port。
- 适配器负责把本地文件、数据库或 API 转换为 `tools.data` 标准对象。
- `main.py` 用 `build_workspace_gateway(settings.DATA_ROOT)` 组装真实数据 Gateway，并暴露 `build_backtest` 工厂给 CLI。
- 策略不读取供应商字段，不直接连接数据库或调用 API。