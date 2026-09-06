"""Downloader protocol and raw-volume writer for the workspace parquet layout.

The downloader is the *write* side of the data domain: it pulls raw vendor
frames and lands them in the exact physical layout that
``data.adapters.workspace_data`` reads (the layout declared by each
``MigrationMapping.physical_sources``).  Vendor fields stay vendor-native here;
all semantic conversion happens in the adapters.

    vendor API -> Downloader -> raw parquet volume -> Adapter -> DataGateway

This module also carries the shared plumbing every scope fetcher relies on:
month iteration, exponential-backoff retries and offset pagination (all
ported from the legacy workspace downloader infra).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Protocol, Sequence, runtime_checkable

import pandas as pd

from data.adapters.workspace_data.mappings import MAPPINGS_BY_DATASET

logger = logging.getLogger(__name__)

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


# ---------------------------------------------------------------------------
# shared plumbing (ported from the legacy downloader infra)
# ---------------------------------------------------------------------------

def retry_call(
    fn: Callable[..., Any],
    *args: Any,
    logger: Optional[logging.Logger] = None,
    label: str = "",
    max_retries: int = 3,
    base_sleep: float = 2.0,
    **kwargs: Any,
) -> Any:
    """Call a vendor API with exponential-backoff retries; ``None`` when exhausted."""
    if logger is None:
        logger = logging.getLogger(__name__)
    for attempt in range(max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if attempt >= max_retries:
                logger.error("[%s] 重试 %d 次仍失败: %s", label, max_retries, exc)
                return None
            wait = base_sleep * (2**attempt)
            logger.warning(
                "[%s] 第 %d 次失败(%s)，%.1f 秒后重试", label, attempt + 1, exc, wait
            )
            time.sleep(wait)
    return None


def fetch_with_pagination(
    func: Callable[..., pd.DataFrame | None],
    params: Optional[dict[str, Any]] = None,
    limit_param: str = "limit",
    offset_param: str = "offset",
    page_size: int = 5000,
    max_pages: int = 100,
    sleep_between: float = 0.1,
    label: str = "",
) -> pd.DataFrame:
    """Offset-pagination fetch: walks ``offset`` until the vendor returns a short page."""
    params = dict(params or {})
    all_rows: list[pd.DataFrame] = []

    for page in range(max_pages):
        offset = page * page_size
        call_params = {**params, limit_param: page_size, offset_param: offset}

        try:
            df = func(**call_params)
        except Exception as exc:
            logger.warning("[%s] page %d (offset=%d) failed: %s", label, page, offset, exc)
            break

        if df is None or df.empty:
            break

        all_rows.append(df)

        if len(df) < page_size:
            break

        if sleep_between > 0:
            time.sleep(sleep_between)

    if not all_rows:
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    logger.info("[%s] fetched %d rows in %d pages", label, len(result), len(all_rows))
    return result


def iter_months(start_date: str, end_date: str) -> Iterator[str]:
    """Yield ``YYYYMM`` strings covering the inclusive YYYYMMDD range."""
    s = datetime.strptime(start_date, "%Y%m%d")
    e = datetime.strptime(end_date, "%Y%m%d")
    cur = datetime(s.year, s.month, 1)

    while cur <= e:
        yield cur.strftime("%Y%m")
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1)
        else:
            cur = datetime(cur.year, cur.month + 1, 1)


def get_month_range(ym: str, end_date: str) -> tuple[str, str]:
    """Clamp month ``ym`` (``YYYYMM``) against ``end_date``; returns (start, end) YYYYMMDD."""
    month_start = ym + "01"
    if ym == end_date[:6]:
        month_end = end_date
    else:
        y, m = int(ym[:4]), int(ym[4:6])
        if m == 12:
            next_month = datetime(y + 1, 1, 1)
        else:
            next_month = datetime(y, m + 1, 1)
        month_end = (next_month - timedelta(days=1)).strftime("%Y%m%d")
    return month_start, month_end


def quarter_end_dates(start_date: str, end_date: str) -> list[str]:
    """Report-period end dates (0331/0630/0930/1231) inside the inclusive range."""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    month_ends = {3: (3, 31), 6: (6, 30), 9: (9, 30), 12: (12, 31)}
    quarters = []
    for year in range(start.year, end.year + 1):
        for month in (3, 6, 9, 12):
            m, d = month_ends[month]
            q_end = datetime(year, m, d)
            if start <= q_end <= end:
                quarters.append(q_end.strftime("%Y%m%d"))
    return quarters


def report_periods(start_date: str, end_date: str) -> list[str]:
    """Report periods (``YYYYMMDD`` quarter ends) for the VIP-period APIs, as strings."""
    quarters = ("0331", "0630", "0930", "1231")
    periods = [
        yq
        for yq in (
            str(year) + q
            for year in range(int(start_date[:4]), int(end_date[:4]) + 1)
            for q in quarters
        )
        if start_date <= yq <= end_date
    ]
    return periods


# ---------------------------------------------------------------------------
# raw volume writer
# ---------------------------------------------------------------------------


class RawVolumeWriter:
    """Lands raw frames into the legacy parquet layout under ``root``.

    Target paths are resolved from the dataset's ``MigrationMapping``, so the
    writer and the readers stay aligned by construction:

    - ``{root}/{file}.parquet``     global snapshot tables (stock_basic, ...)
    - ``{year}/kline.parquet``      rows grouped by year, merged + deduped
    - ``{year}/{MMDD}/stock.parquet``  rows grouped by trading day

    Repeated writes merge into the existing file; ``dedup_keys`` selects the
    business key (keep="last", so re-downloads refresh existing rows), while
    the default deduplicates on the full row.
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
        date_column: str | None = "trade_date",
        dedup_keys: Sequence[str] | None = None,
        dedupe: bool = True,
    ) -> list[Path]:
        """Write a raw frame for ``scope`` and return the touched file paths."""
        dataset = dataset_for_scope(scope)
        if dataset is None:
            raise ValueError(f"unknown legacy scope {scope!r} (no MigrationMapping)")
        return self.write_dataset(
            dataset,
            frame,
            date_column=date_column,
            dedup_keys=dedup_keys,
            dedupe=dedupe,
        )

    def write_dataset(
        self,
        dataset: str,
        frame: pd.DataFrame,
        *,
        date_column: str | None = "trade_date",
        dedup_keys: Sequence[str] | None = None,
        dedupe: bool = True,
    ) -> list[Path]:
        mapping = MAPPINGS_BY_DATASET.get(dataset)
        if mapping is None:
            raise ValueError(f"no MigrationMapping for dataset {dataset!r}")
        template = self._template(dataset, mapping.physical_sources)
        if template is None:
            raise ValueError(
                f"dataset {dataset!r} has no physical source template "
                f"(physical_sources={mapping.physical_sources})"
            )

        if frame.empty:
            return []

        keys = list(dedup_keys) if dedup_keys else None

        # global snapshot: one file at {root}/{file}, no date grouping
        if "{year}" not in template:
            target = self._root / template.split("{root}/", 1)[1]
            target.parent.mkdir(parents=True, exist_ok=True)
            merged = self._merge(target, frame, keys, dedupe)
            merged.to_parquet(target, index=False)
            return [target]

        if date_column is None or date_column not in frame.columns:
            raise ValueError(
                f"dataset {dataset!r} needs date column {date_column!r} for the "
                f"yearly layout {template!r}; got columns {list(frame.columns)}"
            )

        written: list[Path] = []
        per_day = "{MMDD}" in template
        suffix = template.split("{year}/", 1)[1]
        date_text = frame[date_column].astype(str).str.replace(r"\D", "", regex=True)
        group_key = date_text.str.slice(0, 8) if per_day else date_text.str.slice(0, 4)
        for key, group in frame.groupby(group_key, sort=True):
            if per_day:
                target = self._root / key[:4] / key[4:8] / suffix.split("{MMDD}/", 1)[1]
            else:
                target = self._root / key / suffix
            target.parent.mkdir(parents=True, exist_ok=True)
            merged = self._merge(target, group, keys, dedupe)
            merged.to_parquet(target, index=False)
            written.append(target)
        return written

    @staticmethod
    def _template(dataset: str, sources: tuple[str, ...]) -> str | None:
        yearly = next((src for src in sources if "{year}" in src), None)
        if yearly is not None:
            return yearly
        return next((src for src in sources if src.startswith("{root}/")), None)

    def _merge(
        self,
        target: Path,
        payload: pd.DataFrame,
        dedup_keys: list[str] | None,
        dedupe: bool,
    ) -> pd.DataFrame:
        if target.is_file():
            existing = pd.read_parquet(target)
            missing = [k for k in (dedup_keys or []) if k not in existing.columns or k not in payload.columns]
            if missing:
                raise ValueError(
                    f"dedup keys {missing} missing from existing file or payload: {target}"
                )
            payload = pd.concat([existing, payload], ignore_index=True)
        if dedupe:
            payload = payload.drop_duplicates(subset=dedup_keys, keep="last")
        return payload


__all__ = [
    "Downloader",
    "RawVolumeWriter",
    "dataset_for_scope",
    "fetch_with_pagination",
    "get_month_range",
    "iter_months",
    "quarter_end_dates",
    "report_periods",
    "retry_call",
]
