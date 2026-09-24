"""基于旧版 Tushare 布局的 parquet 数据卷的工作区数据适配器。

一期覆盖范围（核心行情链）：``calendar.session``、``instrument.master``、
``industry.membership``、``market.bar``、``index.bar``、``fund.bar``、
``market.daily_metric``、``market.trade``、``market.quote`` 以及
``market.daily_snapshot`` 复合视图。全部 37 个首发数据集都有可审计的
:data:`MIGRATION_MAPPINGS`；没有物理数据源或没有二期适配器的数据集，
会在网关处以 ``UnsupportedDatasetError`` 失败。
"""

from data.adapters.workspace_data.composite import DailySnapshotCompositeAdapter
from data.adapters.workspace_data.gateway import DEFAULT_DATA_ROOT, build_workspace_gateway
from data.adapters.workspace_data.historical import (
    WorkspaceBarAdapter,
    WorkspaceCalendarAdapter,
    WorkspaceTableAdapter,
)
from data.adapters.workspace_data.mappings import (
    MAPPINGS_BY_DATASET,
    MIGRATION_MAPPINGS,
    MigrationMapping,
)
from data.adapters.workspace_data.ticks import WorkspaceTickAdapter

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
