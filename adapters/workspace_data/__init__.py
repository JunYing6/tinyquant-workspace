"""Workspace data adapters over the legacy Tushare-layout parquet volume.

Phase-1 coverage (core market chain): ``calendar.session``,
``instrument.master``, ``industry.membership``, ``market.bar``, ``index.bar``,
``fund.bar``, ``market.daily_metric``, ``market.trade``, ``market.quote`` and
the ``market.daily_snapshot`` composite view.  All 37 first-release datasets
have auditable :data:`MIGRATION_MAPPINGS`; datasets without a physical source
or a phase-2 adapter fail with ``UnsupportedDatasetError`` at the gateway.
"""

from adapters.workspace_data.composite import DailySnapshotCompositeAdapter
from adapters.workspace_data.gateway import DEFAULT_DATA_ROOT, build_workspace_gateway
from adapters.workspace_data.historical import (
    WorkspaceBarAdapter,
    WorkspaceCalendarAdapter,
    WorkspaceTableAdapter,
)
from adapters.workspace_data.mappings import (
    MAPPINGS_BY_DATASET,
    MIGRATION_MAPPINGS,
    MigrationMapping,
)
from adapters.workspace_data.ticks import WorkspaceTickAdapter

__all__ = [
    "DEFAULT_DATA_ROOT",
    "DailySnapshotCompositeAdapter",
    "MAPPINGS_BY_DATASET",
    "MIGRATION_MAPPINGS",
    "MigrationMapping",
    "WorkspaceBarAdapter",
    "WorkspaceCalendarAdapter",
    "WorkspaceTableAdapter",
    "WorkspaceTickAdapter",
    "build_workspace_gateway",
]
