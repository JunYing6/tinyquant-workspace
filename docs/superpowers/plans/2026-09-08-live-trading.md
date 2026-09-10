# 实盘链路 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `tinyquant_workspace` 实现完整实盘链路：实时行情走 `DataGateway.subscribe/poll`（`RealtimeDataPort` 适配器 + 可注入行情客户端）、可插拔券商执行器（Paper/GM）、`trading_nodes/live.py` 工厂；在 `tinyquant` 框架加 `live` 向导命令。

**Architecture:** 行情 `QuoteClient`（jvquant/mock）→ `WorkspaceRealtimeAdapter` 注册进 `DataGateway` 的 push/poll；券商 `TradeExecutor`（paper/gm）可插拔；`RealTimeTradeEngine` 串起三者；`live` 命令仿 `backtest` 向导从 `trading_nodes.live` 读注册表。

**Tech Stack:** Python 3.11+ / rich / prompt_toolkit / pytest / (可选 websockets 用于 jvquant)。

参考设计：`tinyquant_workspace/docs/superpowers/specs/2026-09-08-live-trading-design.md`。
框架命令在 `tinyquant\tinyquant` 下用 `.venv\Scripts\python.exe`；工作区用 `D:\Apps\Python312` + `PYTHONPATH=tinyquant\src;workspace`。

---

### Task 1: 工作区配置（secrets + settings）

**Files:**
- Modify: `tinyquant_workspace/config/secrets.py`, `config/secrets.example.py`, `config/settings.py`

- [ ] **Step 1: secrets 增加字段**

`secrets.py` 与 `secrets.example.py` 各加：
```python
JVQUANT_TOKEN = ""
GM_TOKEN = ""
```

- [ ] **Step 2: settings 增加字段**

```python
REALTIME_QUOTE_CLIENT = "data.realtime.jvquant:JvQuantQuoteClient"
LIVE_EXECUTOR_ROUTE = "paper"
LIVE_INITIAL_CAPITAL = 1_000_000.0
LIVE_MARKET = "CN"
```

- [ ] **Step 3: 验证**

Run: `python -c "from config import settings, secrets; print(settings.LIVE_EXECUTOR_ROUTE)"`
Expected: `paper`

- [ ] **Step 4: 提交（工作区）**

```bash
git add config/secrets.py config/secrets.example.py config/settings.py
git commit -m "feat(config): add live trading settings and secret token fields"
```

---

### Task 2: 行情客户端（base / mock / jvquant）

**Files:**
- Create: `tinyquant_workspace/data/realtime/__init__.py`, `base.py`, `mock.py`, `jvquant.py`

- [ ] **Step 1: `base.py` 定义与失败测试无关；写 `mock.py` 的测试**

新建 `data/realtime/base.py`：
```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class Level1Tick:
    code: str
    name: str
    time: str
    price: float
    change: float
    volume: int
    amount: float
    bid5: list[tuple[int, float]] = field(default_factory=list)
    ask5: list[tuple[int, float]] = field(default_factory=list)
    trade_date: Optional[str] = None
```

- [ ] **Step 2: `mock.py`**

`MockQuoteClient`：实现 `subscribe/unsubscribe/start/stop` 与 `on_tick` 回调；`emit(ticks)` 让测试注入 `Level1Tick` 并转发给 `on_tick`。

- [ ] **Step 3: 写 `tests/test_realtime_mock.py` 并跑通**

```python
def test_mock_quote_client_delivers_ticks() -> None:
    client = MockQuoteClient()
    received = []
    def on_tick(tick): received.append(tick)
    client.on_tick = on_tick
    client.emit(Level1Tick(code="600519", name="x", time="09:30:00",
                           price=100.0, change=0.0, volume=100, amount=10000.0))
    assert received and received[0].code == "600519"
```

- [ ] **Step 4: `jvquant.py`**：移植 research `RealTimeQuotesTool` 的核心（`get_server/connect/disconnect/subscribe/_receive_loop/_parse_level1/_split_provider_timestamp/run_forever`），构造读 `secrets.JVQUANT_TOKEN`；`on_tick` setter 暴露。

- [ ] **Step 5: 提交（工作区）**

```bash
git add data/realtime tests/test_realtime_mock.py
git commit -m "feat(realtime): add quote client protocol with mock and jvquant implementations"
```

---

### Task 3: `WorkspaceRealtimeAdapter`（RealtimeDataPort）

**Files:**
- Create: `tinyquant_workspace/data/adapters/workspace_data/realtime.py`
- Test: `tests/test_realtime_adapter.py`

- [ ] **Step 1: 写失败测试**

用 `MockQuoteClient` 驱动，断言 `subscribe` 后 emit tick 会触达 sink 且类型为 `TradeTick`/`QuoteTick`、`Subscription` 可取消。

- [ ] **Step 2: 实现适配器**

```python
from tools.data import Subscription, StreamEvent, TradeTick, QuoteTick, PriceLevel
```

`WorkspaceRealtimeAdapter(RealtimeDataPort)`：构造 `(quote_client, market="CN")`；`subscribe(request, sink, control_sink=None)` 注册 sink、绑定 `quote_client.on_tick` 到转换（`Level1Tick`→`TradeTick` 或 `QuoteTick`，按 `request.dataset`）、按 `request.instruments` 订阅，返回 `Subscription`；`poll(request)` 从客户端缓冲取事件。

- [ ] **Step 3: 跑通测试**

Run: `pytest tests/test_realtime_adapter.py -q` → PASS

- [ ] **Step 4: 提交（工作区）**

---

### Task 4: 网关实时绑定

**Files:**
- Modify: `tinyquant_workspace/data/adapters/workspace_data/gateway.py`
- Test: `tests/test_realtime_adapter.py`

- [ ] **Step 1: 加 `realtime_client` 参数**

`build_workspace_gateway(..., realtime_client: QuoteClient | None = None)`：当传入时，为 `market.trade`/`market.quote` 增加 `WorkspaceRealtimeAdapter` 绑定，modes `("push","poll")`，优先级高于历史绑定。

- [ ] **Step 2: 测试**：传入 mock client，`gateway.subscribe(StreamRequest(dataset="market.trade", instruments=(...)))` 收到 tick。

- [ ] **Step 3: 提交（工作区）**

---

### Task 5: 券商执行器 + `build_live`

**Files:**
- Create: `tinyquant_workspace/trading_nodes/live.py`
- Test: `tests/test_live.py`

- [ ] **Step 1: 写失败测试**

`PaperTradeExecutor`: `buy("600519",100,price=10.0)` 后 `get_positions()` 含该仓位、`get_account()` 现金减少；`build_executor("paper")` 返回实例。

- [ ] **Step 2: 实现 `trading_nodes/live.py`**

- `PaperTradeExecutor`：实现 `TradeExecutor` 协议（connect/disconnect 空、buy/sell 更新本地持仓/现金、get_positions/get_account）。
- `GMTradeExecutor`：包裹移植 gm 调用（构造需 `config["token"]`/`mode`），未安装 gm 时报 `RuntimeError`。
- `EXECUTORS = {"paper": PaperTradeExecutor, "gm": GMTradeExecutor}`，`build_executor(route, config=None)`。
- `build_live(strategy, gateway, route="paper", initial_capital=1_000_000, executor_config=None)`：构造执行器并返回 `RealTimeTradeEngine(strategy, gateway, executor, initial_capital=...)`。

- [ ] **Step 3: 跑通测试**

Run: `pytest tests/test_live.py -q` → PASS

- [ ] **Step 4: 提交（工作区）**

---

### Task 6: 框架 `live` 向导命令

**Files:**
- Modify: `tinyquant/src/tinyquant_cli/commands/__init__.py`
- Create: `tinyquant/src/tinyquant_cli/live.py`
- Test: `tinyquant/tests/cli/test_live.py`

- [ ] **Step 1: 写失败测试**（`live` 注册、`run_live_wizard` 纯函数：解析行情源/执行器选项）

- [ ] **Step 2: 实现 `tinyquant_cli/live.py`**

`run_live_wizard(console, state)`：读 `trading_nodes.backtests.BACKTESTS` 选策略、读 `trading_nodes.live` 的 `QUOTE_SOURCES`/`LIVE_EXECUTORS` 选行情源与执行器、问初始资金，调用工作区 `build_live` 并 `start()`。纯函数（解析选项）可测。

- [ ] **Step 3: 注册 `live` 命令**（`commands/__init__.py`）

- [ ] **Step 4: 跑通测试 + 提交（框架）**

```bash
python -m pytest tests/cli/test_live.py -q
git add src/tinyquant_cli/live.py src/tinyquant_cli/commands/__init__.py tests/cli/test_live.py
git commit -m "feat(cli): add interactive live trading wizard"
```

---

### Task 7: 全量回归 + 端到端冒烟

**Files:** 无新增

- [ ] **Step 1: 框架全量**

Run: `python -m pytest -q`（除既有 `test_doctor_reports_core_runtime_status` 无关失败外全绿）

- [ ] **Step 2: 工作区冒烟（paper + mock，无 token/网络）**

```python
from trading_nodes.live import build_live
engine = build_live(strategy, build_workspace_gateway(realtime_client=MockQuoteClient()), route="paper")
engine.start(); engine.stop()
```

Expected: 无异常启动/停止。

- [ ] **Step 3: 提交修正**（如有）