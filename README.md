# tinyquant-workspace

用户端策略工作区，用于编写具体量化节点并组装真实数据适配器。框架 `tinyquant` 只发布核心运行时与通用 `tq` CLI，不内置任何具体策略、Mind、Stream 或数据适配器；具体的因子、选股器、择时器、风控、策略、Stream、回测/实盘注册表与真实数据链路都写在这里。

## 目录

```text
trading_nodes/        具体因子、选股器、择时器、风控、策略、Mind、Stream
├── backtests.py      回测注册表（BACKTESTS + build_* 工厂，供 tq backtest 向导）
└── live.py           实盘（Paper/GM 执行器 + build_live/build_realtime_gateway，供 tq live 向导）
data/
├── realtime/         实时行情客户端（QuoteClient 协议：mock / jvquant WebSocket）
├── downloader/       Tushare 数据下载：vendor API -> duckdb 整合卷
│   └── scopes/       30 类数据范围（日线/分钟/财务/资金流/指数/日历等）
├── adapters/
│   └── workspace_data/  数据适配器：日历/主数据/行业/K线/日线指标/Tick/Snapshot/实时 push-poll
└── processing/       二级加工：标准数据集 -> 派生数据集（指标计算）
config/               配置（settings.py、release_settings.py、secrets.py(gitignored)）
main.py               CLI 启动入口（python main.py 进入 tq 工作台）
excel_reports/        Excel 报告默认输出目录（内容由 .gitignore 忽略）
tests/                用户节点、注册表、实时适配器与集成测试
```

具体节点必须从发布版 `trading_nodes_base` 继承基类、使用发布版 `tools.data` 标准数据类型；具体节点之间互相引用使用用户包 `trading_nodes`。

## 安装

框架从源码（含 CLI）安装进当前 Python：

```powershell
D:\Apps\Python312\python.exe -m pip install -e "D:\ProgramFile\tinyquant\tinyquant[cli]"
D:\Apps\Python312\python.exe -m pip install -e ".[dev]"
```

正式使用时把 `pyproject.toml` 的依赖改为发布的 `tinyquant[cli]`，并安装必需的数据依赖（`[workspace-data]`：duckdb、pandas、pyarrow、polars 等）。

## 配置

配置分散在 `config/`：

- `settings.py` —— 回测区间/资金/股票池、`DATA_ROOT` 数据卷、`DATA_GATEWAY_FACTORY` 网关工厂、实时行情客户端 `REALTIME_QUOTE_CLIENT`、实盘默认 `LIVE_EXECUTOR_ROUTE`/`LIVE_INITIAL_CAPITAL`/`LIVE_MARKET`。
- `release_settings.py` —— 回传给框架组件的值：`EXCEL_OUTPUT_DIR`。
- `secrets.py` —— **敏感凭据（gitignored 不提交）**：`TUSHARE_TOKEN`、`QUOTE_TOKEN`、`JVQUANT_TOKEN`、`GM_TOKEN`。从 `secrets.example.py` 复制并填写真实值。

## 运行

运行 `python main.py` 会自动启动发布版的 `tq` CLI 工作台，并把参数原样转发给 CLI：

```powershell
python main.py                                    # 进入 tq 交互式工作台
python main.py help
python main.py backtest                           # 交互式回测向导
python main.py live                               # 交互式实盘向导
```

`tq` 命令属于已安装的发布版 `tinyquant[cli]`，不需要在本项目内重复实现。

### 回测 `tq backtest`

向导读取 `trading_nodes.backtests.BACKTESTS`，依次选择策略或 Stream、回测项目、日期、资金、模式、是否导出 Excel。每个 `build_*` 工厂通过配置驱动的 `data.adapters.build_gateway()` 组装真实数据网关。

### 实盘 `tq live`

向导读取 `trading_nodes.live` 的行情源与执行器注册表，选择策略/Stream、行情源（mock/jvquant）、执行器（paper/gm）、初始资金，构建实时网关并启动 `RealTimeTradeEngine`。无凭据时可用 `mock` + `paper` 本地跑通整条链路。

## Excel 回测报告

Excel 报告**默认不导出**，在回测向导末尾选择导出时生成（策略：五个 Sheet；Stream：另加成员策略汇总/成员权益曲线/权重演变）。默认输出目录由 `config/release_settings.py` 的 `EXCEL_OUTPUT_DIR` 配置，默认为 `excel_reports/`。

## 数据链路

真实数据网关由 `config/settings.DATA_GATEWAY_FACTORY` 决定（默认 `data.adapters.workspace_data:build_workspace_gateway`）。适配器从 `DATA_ROOT`（duckdb 表 + 每日 parquet 的整合卷）读取历史行情；传入 `realtime_client` 时为 `market.trade`/`market.quote` 增加 push/poll 实时绑定，使实时行情也走统一的 `tools.data` 数据接口。

Tushare 下载器通过 `data/downloader/tushare.py` 与 `scopes/` 拉取原始数据，`migrate.py` / `tick_import.py` 负责把旧卷或分笔导入整合布局。

实时行情客户端在 `data/realtime/`：`mock`（本地模拟）与 `jvquant`（WebSocket，token 从 `secrets.JVQUANT_TOKEN` 读）。

## 开发约定

- `trading_nodes/` 只编写具体节点逻辑，通过标准 `DataRequest` 请求数据。
- 回测/实盘入口统一登记到 `trading_nodes/backtests.py` 与 `trading_nodes/live.py`，不散落在 `main.py`。
- 数据适配器位于 `data/adapters/workspace_data/`，实现 `tools.data` 的历史、实时（`subscribe`/`poll`）与交易日历 Port。
- 敏感凭据只放 `config/secrets.py`，版本库里只提交 `secrets.example.py`。
- 策略不读取供应商字段，不直接连接数据库或调用 API。

## 版本与分支

`main` 是稳定分支，仅收 bugfix、随版本发布；架构演进与新功能在 `dev/1.2.0` 开发线进行，稳定后再合入主版本。