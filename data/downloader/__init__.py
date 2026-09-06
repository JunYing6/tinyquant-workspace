"""Data download subpackage: vendor API -> raw parquet volume.

The raw volume layout mirrors the legacy Tushare storage exactly (the layout
declared by ``MigrationMapping.physical_sources``), so the adapters in
``data.adapters.workspace_data`` can read it without any translation on the
write side.  Vendor fields stay vendor-native (no renaming, no unit
conversion); the client is always injected — nothing here reads tokens.

    import tushare as ts
    from data.downloader import TushareDownloader

    downloader = TushareDownloader(client=ts.pro_api(token), root="E:/ProgramData")
    downloader.download("trade_data/daily", "20240102", "20241231")
    downloader.download("trade_data/tick", "20240102", "20240131",
                        options={"csv_root": "E:/2024"})
"""

from data.downloader.base import Downloader, RawVolumeWriter, dataset_for_scope
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
    "import_date_range",
    "import_single_csv",
    "list_scopes",
    "uncovered_legacy_scopes",
]
