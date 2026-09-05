"""Data download subpackage: vendor API -> raw parquet volume.

The raw volume layout mirrors the legacy Tushare storage exactly (the layout
declared by ``MigrationMapping.physical_sources``), so the adapters in
``data.adapters.workspace_data`` can read it without any translation on the
write side.
"""

from data.downloader.base import Downloader, RawVolumeWriter, dataset_for_scope
from data.downloader.tushare import TushareDownloader

__all__ = ["Downloader", "RawVolumeWriter", "TushareDownloader", "dataset_for_scope"]
