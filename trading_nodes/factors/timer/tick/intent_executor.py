"""信号意图 -> 下单请求 执行因子。

Tick 级流式因子：接收上层（择时/调仓）产出的 SignalIntent，并在行情 tick
到来时把它转成具体的 ExecutionRequest（含价格、数量、订单类型等），作为
真正提交到柜台的下单指令。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from trading_nodes_base.factors import TickTimingFactor
from trading_nodes_base.types import ExecutionRequest, SignalIntent


class IntentExecutorFactor(TickTimingFactor):
    """把 BUY/SELL 信号意图落地为可执行的下单请求。"""

    execution_role = "intent_executor"
    accepted_intent_actions = frozenset({"BUY", "SELL"})

    def __init__(self) -> None:
        super().__init__("intent-executor")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[Any]:
        # 流式因子：无需批量数据查询，直接就绪。
        self._data_clear()
        return []

    def on_tick(self, tick: dict[str, Any], intents: Sequence[SignalIntent] = ()) -> list[ExecutionRequest]:
        # 用当前 tick 的实时价格/时间，把每个信号意图转成可执行的下单请求
        requests = []
        for intent in intents:
            metadata = dict(intent.metadata)
            requests.append(ExecutionRequest(
                intent.code, intent.action, tick.get("time"),
                price=float(tick.get("price", 0.0)),
                volume=metadata.get("volume"),
                sizing_intent=metadata.get("sizing_intent"),
                order_type=metadata.get("order_type"),
                reason=intent.reason,
            ))
        return requests


__all__ = ["IntentExecutorFactor"]
