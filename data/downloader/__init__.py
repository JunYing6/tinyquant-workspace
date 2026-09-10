"""Data download subpackage: vendor API -> raw volume (duckdb tables + per-day parquet).

The raw volume layout is declared by ``MigrationMapping``: duckdb tables for
yearly/global data (``{year}/year.duckdb#kline``, ``{root}/reference.duckdb``
master tables) and per-day parquet for the tick volumes and daily bars, so the
adapters in ``data.adapters.workspace_data`` read it without any translation
on the write side.  Vendor fields stay vendor-native (no renaming, no unit
conversion); the client is always injected — nothing here reads tokens.

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
from data.downloader.pack import derive_multi_day_bars, pack_kline
from data.downloader.tick_import import import_date_range, import_single_csv
from data.downloader.tushare import (
    _SCOPE_FETCHERS,
    NON_VENDOR_SCOPES,
    ScopeSpec,
    TushareDownloader,
    uncovered_legacy_scopes,
)


def list_scopes() -> tuple[str, ...]:
    """All registered legacy scope keys."""
    return tuple(sorted(_SCOPE_FETCHERS))


__all__ = [
    "Downloader",
    "NON_VENDOR_SCOPES",
    "RawVolumeWriter",
    "ScopeSpec",
    "TushareDownloader",
    "dataset_for_scope",
    "derive_multi_day_bars",
    "import_date_range",
    "import_single_csv",
    "list_scopes",
    "merge_duckdb_table",
    "migrate_volume",
    "pack_kline",
    "read_duckdb_table",
    "uncovered_legacy_scopes",
]
