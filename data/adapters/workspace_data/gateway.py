"""工作区数据适配器的网关组装。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tools.data import DataBinding, DataGateway, DataPolicy, default_catalog

from data.adapters.workspace_data.composite import DailySnapshotCompositeAdapter
from data.adapters.workspace_data.historical import (
    WorkspaceBarAdapter,
    WorkspaceCalendarAdapter,
    WorkspaceTableAdapter,
)
from data.adapters.workspace_data.realtime import WorkspaceRealtimeAdapter
from data.adapters.workspace_data.ticks import WorkspaceTickAdapter

DEFAULT_DATA_ROOT = "E:/ProgramData"


def build_workspace_gateway(
    data_root: str | Path | None = None,
    *,
    market: str = "CN",
    policy: DataPolicy | None = None,
    realtime_client: Any = None,
    **gateway_kwargs: Any,
) -> DataGateway:
    """在整合后的工作区数据卷（duckdb 表 + 按日 parquet）之上组装一个 :class:`DataGateway`。

    一期绑定：``calendar.session``、``instrument.master``、
    ``industry.membership``、``market.bar``、``index.bar``、``fund.bar``、
    ``market.daily_metric``、``market.trade``、``market.quote``、
    ``fundamental.income``/``balance``/``cashflow`` 以及 ``market.daily_snapshot``
    复合视图。其余二期数据集（事件、辅助表）以及 ``contract_only`` 数据集尚未绑定，
    请求时以 ``UnsupportedDatasetError`` 失败。

    当提供 ``realtime_client`` 时，``market.trade`` / ``market.quote`` 还会获得由
    :class:`WorkspaceRealtimeAdapter` 提供的实时（push/poll）绑定，这样实盘引擎
    就能通过网关订阅。
    """
    root = Path(data_root) if data_root is not None else _default_root()
    calendar_adapter = WorkspaceCalendarAdapter(root, market)
    bars = WorkspaceBarAdapter(root, market)
    tables = WorkspaceTableAdapter(root, market)
    ticks = WorkspaceTickAdapter(root, market)
    snapshot = DailySnapshotCompositeAdapter(bars, tables)

    def _bind(dataset: str, adapter: Any, priority: int = 1, modes: tuple[str, ...] = ("historical",)):
        return (
            DataBinding(dataset, adapter.descriptor.name, priority, modes),
            adapter.descriptor,
            adapter,
        )

    bindings = [
        _bind("calendar.session", calendar_adapter, modes=("calendar",)),
        _bind("market.bar", bars),
        _bind("index.bar", bars),
        _bind("fund.bar", bars),
        _bind("market.daily_metric", tables),
        _bind("instrument.master", tables),
        _bind("industry.membership", tables),
        _bind("fundamental.income", tables),
        _bind("fundamental.balance", tables),
        _bind("fundamental.cashflow", tables),
        _bind("market.trade", ticks),
        _bind("market.quote", ticks),
        _bind("market.daily_snapshot", snapshot),
    ]
    if realtime_client is not None:
        realtime = WorkspaceRealtimeAdapter(realtime_client, market)
        bindings.append(_bind("market.trade", realtime, priority=2, modes=("push", "poll")))
        bindings.append(_bind("market.quote", realtime, priority=2, modes=("push", "poll")))
    return DataGateway(
        catalog=default_catalog(),
        bindings=bindings,
        policy=policy or DataPolicy(timezone="Asia/Shanghai"),
        **gateway_kwargs,
    )


def _default_root() -> Path:
    try:
        from config import settings

        root = getattr(settings, "DATA_ROOT", None)
        if root:
            return Path(root)
    except Exception:
        pass
    return Path(DEFAULT_DATA_ROOT)


__all__ = ["DEFAULT_DATA_ROOT", "build_workspace_gateway"]
