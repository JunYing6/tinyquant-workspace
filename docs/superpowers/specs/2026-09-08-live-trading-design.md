# 实盘链路设计（实时行情适配器 + 券商执行器 + live 引擎/命令）

日期：2026-09-08

## 背景与目标

工作区目前只有回测（duckdb/parquet 历史数据），没有实盘能力。目标：搭建完整实盘链路，实时行情走框架的
adapter 数据接口（`DataGateway.subscribe/poll`），券商下单走 `TradeExecutor` 协议，提供 `live` 命令与启动工厂。

三个子项目：
1. 实时行情适配器（`RealtimeDataPort`，行情走 DataGateway 实时接口）
2. 券商执行器（可插拔，Paper 模拟 + 掘金真实现）
3. 实盘引擎 + `live` 命令

## 架构总览

```text
实时行情:  QuoteClient(jvquant/mock) ──> WorkspaceRealtimeAdapter(RealtimeDataPort)
                                     ──> DataGateway.subscribe/poll (market.trade/quote)
券商执行:  TradeExecutor(Paper/GM) ──> tools.trade.providers.TradeExecutor 协议
引擎启动:  RealTimeTradeEngine(strategy, gateway, trade_executor)
配置:      secrets.py 存 token；settings.py 选行情客户端/执行器
入口:      tq live（向导）或 trading_nodes/live.build_live()
```

## 配置

### `config/secrets.py`（gitignored）新增
- `JVQUANT_TOKEN = ""`：实时行情 token
- `GM_TOKEN = ""`：掘金券商 token

### `config/settings.py` 新增
- `REALTIME_QUOTE_CLIENT = "data.realtime.jvquant:JvQuantQuoteClient"`：行情客户端 `模块:类`
- `LIVE_EXECUTOR_ROUTE = "paper"`：paper / gm
- `LIVE_INITIAL_CAPITAL = 1_000_000.0`
- `LIVE_MARKET = "CN"`

## 1. 实时行情（走 adapter 数据接口）

### `data/realtime/base.py`
行情客户端协议：
```python
class QuoteClient(Protocol):
    def subscribe(self, *codes: str) -> None: ...
    def unsubscribe(self, *codes: str) -> None: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
    @property
    def on_tick(self) -> Callable[[Level1Tick], None] | None: ...
    @on_tick.setter
    def on_tick(self, fn: Callable[[Level1Tick], None]) -> None: ...
```

`Level1Tick`（dataclass）：`code/name/time/price/change/volume/amount/bid5/ask5/trade_date`。

### `data/realtime/jvquant.py`
移植 research 的 `RealTimeQuotesTool`（jvQuant WebSocket），实现 `QuoteClient`。token 从 `config.secrets.JVQUANT_TOKEN` 读取。

### `data/realtime/mock.py`
`MockQuoteClient`：无 token 本地模拟，按需产出/回放 `Level1Tick`，供测试与无 token 演示。

### `data/adapters/workspace_data/realtime.py`
`WorkspaceRealtimeAdapter(RealtimeDataPort)`：
- 构造：`(quote_client: QuoteClient, market="CN")`
- `subscribe(request, sink, control_sink=None) -> Subscription`：注册 sink，把 `quote_client.on_tick` 绑定到转换函数（`Level1Tick` → `TradeTick`/`QuoteTick`），按 `request.instruments` 订阅，返回 `Subscription`
- `poll(request) -> Iterator[StreamEvent]`：从客户端拉取/回放事件
- 事件转换：行情 tick → `market.trade` 用 `TradeTick`；`market.quote` 用 `QuoteTick`；补齐 `schema_version/event_id/.../quality/provenance` 字段

### 网关装配
`data/adapters/workspace_data/gateway.py` 的 `build_workspace_gateway` 增加可选参数 `realtime_client: QuoteClient | None = None`：
- 为 `None` 时行为不变（历史模式，`market.trade/quote` 仍由 `WorkspaceTickAdapter` 服务）。
- 传入时，为 `market.trade`/`market.quote` 增加 `WorkspaceRealtimeAdapter` 绑定，modes 含 `("push","poll")`，优先级高于历史绑定；`DataGateway.subscribe/poll` 因此路由到实时适配器。

## 2. 券商执行器

### `trading_nodes/live.py`
实现 `tools.trade.providers.TradeExecutor` 协议：

- `PaperTradeExecutor`：本地模拟撮合。`connect/disconnect` 空操作；`buy/sell` 记录并更新本地账户/持仓；`get_positions/get_account` 返回本地状态。不依赖券商与 token。
- `GMTradeExecutor`：包裹移植的掘金接口。`config['token']`（GM_TOKEN）、`config['mode']`（live/sim）；`buy/sell` 调 `gm.api.order_volume`；`get_positions/get_account` 调 `gm.api`。

注册表：
```python
EXECUTORS = {
    "paper": PaperTradeExecutor,
    "gm": GMTradeExecutor,
}

def build_executor(route: str, config: dict | None = None) -> TradeExecutor: ...
```

### 引擎工厂
```python
def build_live(strategy, gateway, route="paper", initial_capital=1_000_000,
               executor_config=None) -> RealTimeTradeEngine:
    executor = build_executor(route, executor_config)
    return RealTimeTradeEngine(strategy, gateway, executor, initial_capital=initial_capital)
```

## 3. CLI `live` 命令

注册 `live` 命令（`commands/__init__.py`），handler `run_live_wizard`（`tinyquant_cli/live.py`，模式仿 `backtest` 向导，含可测试纯函数）：
1. 选择策略/Stream（复用 `trading_nodes.backtests.BACKTESTS` 清单）
2. 选择行情源（jvquant / mock）
3. 选择执行器（paper / gm）
4. 初始资金
5. 构造实时网关（`build_workspace_gateway(realtime_client=...)`）→ `build_live(...)` → `engine.start()`

`live` 向导的行情源与执行器清单同样做成配置/注册表驱动，便于扩展。

## 4. 测试

- 框架端：`live` 向导纯函数（解析行情源/执行器选项、资金校验）单测。
- 工作区端：
  - `WorkspaceRealtimeAdapter`：用 `MockQuoteClient` 验证 `subscribe`/`poll` 产出 `TradeTick`/`QuoteTick`，`Subscription` 可取消。
  - `PaperTradeExecutor`：buy/sell 正确更新账户/持仓。
  - `build_live` paper 模式：无需 token/网络即可构造并 start/stop `RealTimeTradeEngine`（用 mock 行情客户端）。
- 手动：`tq live` 走 paper + mock 整链路。

## 非目标

- 不迁移 research 的 `TradeManager`/`GMTradeInterface` 整套类层次；只在 `trading_nodes/live.py` 内按 `TradeExecutor` 协议实现 Paper 与 GM 两个执行器。
- 不把真实券商下单跑通到线上（需真实 token/网络），GM 只做移植与接口正确性；真实验证由用户提供 token 后进行。
- 不改动 `tinyquant` 框架的 `RealTimeTradeEngine` 与 `TradeExecutor` 协议（复用现成）。
