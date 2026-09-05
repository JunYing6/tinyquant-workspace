"""Shared parquet IO and market-calendar helpers for the workspace adapters."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd

CST = timezone(timedelta(hours=8))  # China Standard Time, no DST
MARKET = "CN"
SESSION_OPEN = time(9, 30)
SESSION_CLOSE = time(15, 0)

ASSET_TYPE_MAP = {"stock": "equity", "index": "index", "fund": "fund"}
STATUS_MAP = {"L": "listed", "D": "delisted", "P": "paused"}


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


def read_frame(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    """Read one parquet file into a pandas frame (pyarrow backend)."""
    return pd.read_parquet(path, columns=columns)


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
    "SESSION_CLOSE",
    "SESSION_OPEN",
    "STATUS_MAP",
    "announcement_close",
    "date_range_days",
    "parse_yyyymmdd",
    "read_frame",
    "resolve_year_files",
    "session_bounds",
    "source_revision",
    "to_yyyymmdd",
]
