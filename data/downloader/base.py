"""Downloader protocol and raw-volume writer for the workspace layout.

The downloader is the *write* side of the data domain: it pulls raw vendor
frames and lands them in the exact physical layout that
``data.adapters.workspace_data`` reads (the layout declared by each
``MigrationMapping``).  Vendor fields stay vendor-native here; all semantic
conversion happens in the adapters.

    vendor API -> Downloader -> raw volume -> Adapter -> DataGateway

The physical layout the writer produces:

- ``{root}/reference.duckdb``      one table per non-date-scoped table
- ``{root}/{year}/year.duckdb``    one table per date-scoped yearly table
- ``{root}/{year}/{MMDD}/*.parquet``  per-day files (tick volumes, daily bars)

Templates declare the target as ``{root}/reference.duckdb#trade_date`` or
``{year}/year.duckdb#daily_basic`` (``#`` names the table); parquet templates
keep the ``{year}``/``{MMDD}`` path forms.  Repeated writes merge into the
existing table/file; ``dedup_keys`` selects the business key (keep="last", so
re-downloads refresh existing rows), while the default deduplicates on the
full row.

This module also carries the shared plumbing every scope fetcher relies on:
month iteration, exponential-backoff retries and offset pagination (all
ported from the legacy workspace downloader infra).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Protocol, Sequence, runtime_checkable

import duckdb
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
# duckdb table IO
# ---------------------------------------------------------------------------


def duckdb_table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0])


def read_duckdb_table(
    db_path: str | Path,
    table: str,
    columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Read one table from a duckdb file into a pandas frame."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        projection = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        return con.execute(f'SELECT {projection} FROM "{table}"').df()
    finally:
        con.close()


def merge_duckdb_table(
    db_path: str | Path,
    table: str,
    frame: pd.DataFrame,
    dedup_keys: Sequence[str] | None = None,
    dedupe: bool = True,
) -> int:
    """Merge ``frame`` into ``table`` (business-key dedupe, keep last); returns row count.

    The whole table is rewritten atomically per call: read existing rows,
    concat + dedupe, then ``CREATE OR REPLACE TABLE``.  The connection is
    opened and closed per call so readers are only blocked for the rewrite.
    """
    if frame.empty:
        return 0
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    keys = list(dedup_keys) if dedup_keys else None

    con = duckdb.connect(str(db_path))
    try:
        combined = frame
        if duckdb_table_exists(con, table):
            existing = con.execute(f'SELECT * FROM "{table}"').df()
            missing = [k for k in (keys or []) if k not in existing.columns or k not in combined.columns]
            if missing:
                raise ValueError(
                    f"dedup keys {missing} missing from existing table {table!r} or payload: {db_path}"
                )
            combined = pd.concat([existing, combined], ignore_index=True)
        if dedupe:
            combined = combined.drop_duplicates(subset=keys, keep="last")
        con.register("_payload_df", combined)
        con.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM _payload_df')
        con.unregister("_payload_df")
        return len(combined)
    finally:
        con.close()


@dataclass(frozen=True)
class WriteTarget:
    """A parsed physical write target."""

    kind: str  # "parquet" | "duckdb"
    scoped: str  # "year" | "root"
    template: str  # original template, e.g. "{year}/year.duckdb#kline"
    per_day: bool  # parquet only: {MMDD} path form
    table: str | None  # duckdb only


def parse_target(template: str) -> WriteTarget:
    """Parse a write-template string into its kind/scope/table parts."""
    if "#" in template:
        path, table = template.split("#", 1)
        return WriteTarget(kind="duckdb", scoped="root" if "{root}" in template else "year",
                           template=template, per_day=False, table=table)
    return WriteTarget(
        kind="parquet",
        scoped="root" if "{root}" in template else "year",
        template=template,
        per_day="{MMDD}" in template,
        table=None,
    )


# ---------------------------------------------------------------------------
# raw volume writer
# ---------------------------------------------------------------------------


class RawVolumeWriter:
    """Lands raw frames into the legacy workspace layout under ``root``.

    Target paths/tables are resolved from the dataset's ``MigrationMapping``
    (``write_targets`` first, then ``physical_sources``), so the writer and the
    readers stay aligned by construction:

    - ``{root}/reference.duckdb#trade_date``   global snapshot tables
    - ``{year}/year.duckdb#daily_basic``       date-scoped yearly tables
    - ``{year}/{MMDD}/kline.parquet``          rows grouped by trading day

    Repeated writes merge into the existing table/file; ``dedup_keys`` selects
    the business key (keep="last", so re-downloads refresh existing rows),
    while the default deduplicates on the full row.
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
        template = self._write_template(dataset, mapping)
        if template is None:
            raise ValueError(
                f"dataset {dataset!r} has no physical write template "
                f"(physical_sources={mapping.physical_sources}, write_targets={mapping.write_targets})"
            )

        if frame.empty:
            return []

        keys = list(dedup_keys) if dedup_keys else None
        target = parse_target(template)

        if target.scoped == "root":
            return self._write_root_frame(target, frame, keys, dedupe)

        if date_column is None or date_column not in frame.columns:
            raise ValueError(
                f"dataset {dataset!r} needs date column {date_column!r} for the "
                f"yearly layout {template!r}; got columns {list(frame.columns)}"
            )
        return self._write_year_frames(target, frame, date_column, keys, dedupe)

    # -- internals ----------------------------------------------------------

    @staticmethod
    def _write_template(dataset: str, mapping: Any) -> str | None:
        candidates = tuple(mapping.write_targets) or tuple(mapping.physical_sources)
        # per-day parquet first, then yearly, then root-scoped
        for template in candidates:
            if "{MMDD}" in template:
                return template
        for template in candidates:
            if "{year}" in template:
                return template
        return next((src for src in candidates if src.startswith("{root}/")), None)

    def _write_root_frame(
        self,
        target: WriteTarget,
        frame: pd.DataFrame,
        keys: list[str] | None,
        dedupe: bool,
    ) -> list[Path]:
        if target.kind == "duckdb":
            db_path = self._root / target.template.split("{root}/", 1)[1].split("#", 1)[0]
            merge_duckdb_table(db_path, target.table, frame, keys, dedupe)
            return [db_path]
        target_path = self._root / target.template.split("{root}/", 1)[1]
        target_path.parent.mkdir(parents=True, exist_ok=True)
        merged = _merge_frame(target_path, frame, keys, dedupe)
        merged.to_parquet(target_path, index=False)
        return [target_path]

    def _write_year_frames(
        self,
        target: WriteTarget,
        frame: pd.DataFrame,
        date_column: str,
        keys: list[str] | None,
        dedupe: bool,
    ) -> list[Path]:
        date_text = frame[date_column].astype(str).str.replace(r"\D", "", regex=True)
        group_key = date_text.str.slice(0, 8) if target.per_day else date_text.str.slice(0, 4)
        written: list[Path] = []
        for key, group in frame.groupby(group_key, sort=True):
            if target.kind == "duckdb":
                db_path = self._root / key[:4] / target.template.split("{year}/", 1)[1].split("#", 1)[0]
                merge_duckdb_table(db_path, target.table, group, keys, dedupe)
                written.append(db_path)
                continue
            if target.per_day:
                target_path = self._root / key[:4] / key[4:8] / target.template.split("{year}/", 1)[1].split("{MMDD}/", 1)[1]
            else:
                target_path = self._root / key / target.template.split("{year}/", 1)[1]
            target_path.parent.mkdir(parents=True, exist_ok=True)
            merged = _merge_frame(target_path, group, keys, dedupe)
            merged.to_parquet(target_path, index=False)
            written.append(target_path)
        return written


def _merge_frame(
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
    "WriteTarget",
    "dataset_for_scope",
    "duckdb_table_exists",
    "fetch_with_pagination",
    "get_month_range",
    "iter_months",
    "merge_duckdb_table",
    "parse_target",
    "quarter_end_dates",
    "read_duckdb_table",
    "report_periods",
    "retry_call",
]
