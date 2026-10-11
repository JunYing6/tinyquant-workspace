"""数据下载子包：厂商 API -> 原始卷（duckdb 表 + 按日 parquet）。

原始卷布局由 ``MigrationMapping`` 声明：年度/全局数据用 duckdb 表
（``{year}/year.duckdb#kline``、``{root}/reference.duckdb`` 主表），
分笔量与日线用按日 parquet，因此 ``data.adapters.workspace_data``
中的适配器无需在写入端做任何转换即可读取。厂商字段保持厂商原生
（不重命名、不做单位转换）；客户端始终由调用方注入——此处不读取任何令牌。

    import tushare as ts
    from config import secrets
    from data.downloader import TushareDownloader

    downloader = TushareDownloader(client=ts.pro_api(secrets.TUSHARE_TOKEN), root="E:/ProgramData")
    downloader.download("trade_data/daily", "20240102", "20241231")
    downloader.download("trade_data/tick", "20240102", "20240131",
                        options={"csv_root": "E:/2024"})
"""

from data.downloader.base import (
    Downloader,
    RawVolumeWriter,
    dataset_for_scope,
    merge_duckdb_table,
    read_duckdb_table,
)
from data.downloader.migrate import migrate_volume
from data.downloader.tick_import import import_date_range, import_single_csv
from data.downloader.tushare import (
    _SCOPE_FETCHERS,
    NON_VENDOR_SCOPES,
    ScopeSpec,
    TushareDownloader,
    uncovered_legacy_scopes,
)


def list_scopes() -> tuple[str, ...]:
    """所有已注册的旧版 scope 键。"""
    return tuple(sorted(_SCOPE_FETCHERS))


__all__ = [
    "Downloader",
    "NON_VENDOR_SCOPES",
    "RawVolumeWriter",
    "ScopeSpec",
    "TushareDownloader",
    "dataset_for_scope",
    "import_date_range",
    "import_single_csv",
    "list_scopes",
    "merge_duckdb_table",
    "migrate_volume",
    "read_duckdb_table",
    "uncovered_legacy_scopes",
]
