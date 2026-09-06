"""Shared data IO and market-calendar helpers for the workspace adapters.

Sources are the consolidated volume layout: duckdb tables for yearly/global
data (``{year}/year.duckdb#kline``, ``{root}/reference.duckdb#stock_basic``,
...) and per-day parquet for the tick volumes.  All conversions follow
``adapters/workspace_data/mappings.py``.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd

CST = timezone(timedelta(hours=8))  # China Standard Time, no DST
MARKET = "CN"
SESSION_OPEN = time(9, 30)
SESSION_CLOSE = time(15, 0)

ASSET_TYPE_MAP = {"stock": "equity", "index": "index", "fund": "fund"}
STATUS_MAP = {"L": "listed", "D": "delisted", "P": "paused"}

REFERENCE_DB = "reference.duckdb"
YEAR_DB = "year.duckdb"


def session_bounds(trading_date: date) -> tuple[datetime, datetime]:
    """Return the (inclusive) session open and close datetimes in CST."""
    return (
        datetime.combine(trading_date, SESSION_OPEN, tzinfo=CST),
        datetime.combine(trading_date, SESSION_CLOSE, tzinfo=CST),
    )


def announcement_close(announcement: date) -> datetime:
    """Conservative PIT rule: date-level announcements become visible at session close."""
    return datetime.combine(announcement, SESSION_CLOSE, tzinfo=CST)


def to_yyyymmdd(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    return str(value)[:8].replace("-", "")


def parse_yyyymmdd(value: object) -> date | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value)
    if len(text) != 8 or not text.isdigit():
        return None
    return datetime.strptime(text, "%Y%m%d").date()


def resolve_year_files(root: Path, filename: str, start: date | None, end: date | None) -> list[Path]:
    """Resolve ``{root}/{year}/{filename}`` files covering the requested range."""
    if not root.is_dir():
        raise FileNotFoundError(f"workspace data root does not exist: {root}")
    year_dirs = sorted(int(p.name) for p in root.iterdir() if p.is_dir() and p.name.isdigit())
    if start is not None:
        first_year = start.year if not isinstance(start, datetime) else start.year
    else:
        first_year = year_dirs[0] if year_dirs else None
    if end is not None:
        last_year = end.year if not isinstance(end, datetime) else end.year
    else:
        last_year = year_dirs[-1] if year_dirs else None
    files: list[Path] = []
    for year in year_dirs:
        if first_year is not None and year < first_year:
            continue
        if last_year is not None and year > last_year:
            continue
        candidate = root / str(year) / filename
        if candidate.is_file():
            files.append(candidate)
    return files


def resolve_year_dbs(root: Path, start: date | None, end: date | None) -> list[Path]:
    """Resolve ``{root}/{year}/year.duckdb`` files covering the requested range."""
    return resolve_year_files(root, YEAR_DB, start, end)


def read_frame(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    """Read one parquet file into a pandas frame (pyarrow backend)."""
    return pd.read_parquet(path, columns=columns)


def read_table(
    db_path: Path,
    table: str,
    columns: list[str] | None = None,
    where: str | None = None,
    params: list | None = None,
) -> pd.DataFrame:
    """Read one table from a duckdb file (read-only, connection per call)."""
    if not db_path.is_file():
        raise FileNotFoundError(f"duckdb data file not found: {db_path}")
    projection = ", ".join(f'"{c}"' for c in columns) if columns else "*"
    sql = f'SELECT {projection} FROM "{table}"'
    if where:
        sql += f" WHERE {where}"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(sql, params or []).df()
    finally:
        con.close()


def table_exists(db_path: Path, table: str) -> bool:
    """Whether ``table`` exists in the duckdb file at ``db_path``."""
    if not db_path.is_file():
        return False
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = ?",
            [table],
        ).fetchone()
        return bool(row and row[0])
    finally:
        con.close()


def source_revision(paths: list[Path]) -> str:
    """Stable data revision: md5 over (relative name, size, mtime) of the sources."""
    digest = hashlib.md5()
    for path in sorted(paths, key=lambda p: str(p)):
        stat = path.stat()
        digest.update(f"{path.name}|{stat.st_size}|{int(stat.st_mtime)}".encode())
    return "wp-" + digest.hexdigest()[:16]


def date_range_days(start: datetime | date | None, end: datetime | date | None) -> list[date]:
    """Days covered by a half-open [start, end) request window.

    ``end`` is exclusive: a midnight end excludes the end day itself.
    """
    if start is None or end is None:
        raise ValueError("both start and end are required to resolve day files")
    start_d = start.date() if isinstance(start, datetime) else start
    if isinstance(end, datetime):
        if end.time() == time(0, 0, 0):
            end_d = (end - timedelta(days=1)).date()
        else:
            end_d = end.date()
    else:
        end_d = end
    if end_d < start_d:
        return []
    days: list[date] = []
    current = start_d
    while current <= end_d:
        days.append(current)
        current += timedelta(days=1)
    return days


__all__ = [
    "ASSET_TYPE_MAP",
    "CST",
    "MARKET",
    "REFERENCE_DB",
    "SESSION_CLOSE",
    "SESSION_OPEN",
    "STATUS_MAP",
    "YEAR_DB",
    "announcement_close",
    "date_range_days",
    "parse_yyyymmdd",
    "read_frame",
    "read_table",
    "resolve_year_dbs",
    "resolve_year_files",
    "session_bounds",
    "source_revision",
    "table_exists",
    "to_yyyymmdd",
]
