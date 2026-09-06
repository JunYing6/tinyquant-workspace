"""Gateway assembly for the workspace data adapters."""

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
from data.adapters.workspace_data.ticks import WorkspaceTickAdapter

DEFAULT_DATA_ROOT = "E:/ProgramData"


def build_workspace_gateway(
    data_root: str | Path | None = None,
    *,
    market: str = "CN",
    policy: DataPolicy | None = None,
    **gateway_kwargs: Any,
) -> DataGateway:
    """Assemble a :class:`DataGateway` over the consolidated workspace volume
    (duckdb tables + per-day parquet).

    Phase-1 bindings: ``calendar.session``, ``instrument.master``,
    ``industry.membership``, ``market.bar``, ``index.bar``, ``fund.bar``,
    ``market.daily_metric``, ``market.trade``, ``market.quote`` and the
    ``market.daily_snapshot`` composite view.  Phase-2 datasets (financials,
    events, auxiliary tables) and ``contract_only`` datasets have no binding
    yet and fail with ``UnsupportedDatasetError`` when requested.
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
        _bind("market.trade", ticks),
        _bind("market.quote", ticks),
        _bind("market.daily_snapshot", snapshot),
    ]
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
