# tinyquant-workspace

用户端策略工作区，用于编写具体量化节点并组装数据适配器。

## 目录

```text
trading_nodes/        具体因子、选股器、择时器、风控、策略、Mind、Stream
data/                 数据域总包（下载 → 适配 → 加工）
├── downloader/       数据下载：vendor API -> 原始 parquet 卷（Tushare 骨架）
├── adapters/         适配接口：原始卷 -> 标准 tools.data 数据集
│   └── workspace_data/  核心行情链（日历/主数据/行业/K线/日线指标/Tick/快照）
└── processing/       二级加工：标准数据集 -> 派生数据集（指标计算）
config/               项目配置（含 DATA_ROOT 数据卷路径）
main.py               Gateway 组装、build_backtest 工厂和直接运行入口
run_multi.py          九策略组合的用户侧回测运行器
tests/                用户节点与集成测试
```

具体节点必须从发布版 `trading_nodes_base` 继承基类、使用发布版 `tools.data` 标准数据类型。具体节点之间互相引用使用用户包 `trading_nodes`。

## 安装库端

开发阶段将 `pyproject.toml` 中的 `tinyquant` 依赖替换为库端本地路径，或先在库端项目安装：

```powershell
D:\Apps\Python312\python.exe -m pip install -e "D:\ProgramFile\QuantZero-1.1"
D:\Apps\Python312\python.exe -m pip install -e ".[dev]"
```

正式使用时改为安装发布的 `tinyquant` 包。

## 运行

直接回测入口：

```powershell
python main.py
```

九策略组合运行器：

```powershell
python run_multi.py
```

通过发布版通用 CLI 运行 `main:build_backtest` 工厂：

```powershell
tq backtest run main:build_backtest --start 20240102 --end 20240103
```

`tq` 属于已安装的发布版 `tinyquant[cli]`，不需要在本项目内重复实现。

## 开发约定

- `trading_nodes/` 只编写具体节点逻辑和标准数据请求。
- `adapters/` 实现 `tools.data` 的历史、实时和交易日历 Port。
- 适配器负责把本地文件、数据库或 API 转换为 `tools.data` 标准对象。
- `main.py` 负责创建 `DataCatalog`、注册 Adapter、组装 `DataGateway` 和启动引擎。
- 策略不读取供应商字段，不直接连接数据库或调用 API。