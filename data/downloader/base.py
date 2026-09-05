"""Downloader protocol and raw-volume writer for the workspace parquet layout.

The downloader is the *write* side of the data domain: it pulls raw vendor
frames and lands them in the exact physical layout that
``data.adapters.workspace_data`` reads (the layout declared by each
``MigrationMapping.physical_sources``).  Vendor fields stay vendor-native here;
all semantic conversion happens in the adapters.

    vendor API -> Downloader -> raw parquet volume -> Adapter -> DataGateway
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, runtime_checkable

import pandas as pd

from data.adapters.workspace_data.mappings import MAPPINGS_BY_DATASET

_DATASET_BY_SCOPE: dict[str, str] = {}
for _m in MAPPINGS_BY_DATASET.values():
    for _scope in _m.legacy_scope:
        _DATASET_BY_SCOPE.setdefault(_scope, _m.dataset)


def dataset_for_scope(scope: str) -> str | None:
    """Reverse lookup: legacy scope (e.g. ``trade_data/daily``) -> dataset name."""
    return _DATASET_BY_SCOPE.get(scope)


@runtime_checkable
class Downloader(Protocol):
    """A vendor fetcher that returns raw (vendor-native) frames."""

    def fetch(self, scope: str, start: str, end: str) -> pd.DataFrame:
        """Fetch ``scope`` rows for the inclusive YYYYMMDD range."""
        ...


class RawVolumeWriter:
    """Lands raw frames into the legacy parquet layout under ``root``.

    Target paths are resolved from the dataset's ``MigrationMapping``, so the
    writer and the readers stay aligned by construction:

    - ``{year}/kline.parquet``     rows grouped by year, merged + deduped
    - ``{year}/{MMDD}/stock.parquet``  rows grouped by trading day
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    @property
    def root(self) -> Path:
        return self._root

    def write_scope(
        self,
        scope: str,
        frame: pd.DataFrame,
        *,
        date_column: str = "trade_date",
        dedupe: bool = True,
    ) -> list[Path]:
        """Write a raw frame for ``scope`` and return the touched file paths."""
        dataset = dataset_for_scope(scope)
        if dataset is None:
            raise ValueError(f"unknown legacy scope {scope!r} (no MigrationMapping)")
        return self.write_dataset(dataset, frame, date_column=date_column, dedupe=dedupe)

    def write_dataset(
        self,
        dataset: str,
        frame: pd.DataFrame,
        *,
        date_column: str = "trade_date",
        dedupe: bool = True,
    ) -> list[Path]:
        mapping = MAPPINGS_BY_DATASET.get(dataset)
        if mapping is None:
            raise ValueError(f"no MigrationMapping for dataset {dataset!r}")
        template = next(
            (src for src in mapping.physical_sources if "{year}" in src),
            None,
        )
        if template is None:
            raise ValueError(
                f"dataset {dataset!r} has no yearly physical source template "
                f"(physical_sources={mapping.physical_sources})"
            )

        written: list[Path] = []
        per_day = "{MMDD}" in template
        for day_value, group in frame.groupby(date_column):
            day_text = str(day_value)
            year = day_text[:4]
            suffix = template.split("{year}/", 1)[1]
            if per_day:
                target = self._root / year / day_text[4:8] / suffix.split("{MMDD}/", 1)[1]
                payload = group
            else:
                target = self._root / year / suffix
                payload = group
            target.parent.mkdir(parents=True, exist_ok=True)
            merged = self._merge(target, payload, dedupe)
            merged.to_parquet(target, index=False)
            written.append(target)
        return written

    def _merge(self, target: Path, payload: pd.DataFrame, dedupe: bool) -> pd.DataFrame:
        if not target.is_file():
            return payload
        existing = pd.read_parquet(target)
        merged = pd.concat([existing, payload], ignore_index=True)
        if dedupe:
            merged = merged.drop_duplicates(keep="last")
        return merged


__all__ = ["Downloader", "RawVolumeWriter", "dataset_for_scope"]
