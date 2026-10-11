"""工作区适配器共享的数据 IO 与交易日历辅助工具。

数据源是整合后的数据卷布局：年度/全局数据用 duckdb 表
（``{year}/year.duckdb#kline``、``{root}/reference.duckdb#stock_basic`` 等），
tick 成交量用每日 parquet。所有转换遵循 ``adapters/workspace_data/mappings.py``。
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import duckdb

CST = timezone(timedelta(hours=8))  # 中国标准时间，无夏令时
MARKET = "CN"
SESSION_OPEN = time(9, 30)
SESSION_CLOSE = time(15, 0)

ASSET_TYPE_MAP = {"stock": "equity", "index": "index", "fund": "fund"}
STATUS_MAP = {"L": "listed", "D": "delisted", "P": "paused"}

REFERENCE_DB = "reference.duckdb"
YEAR_DB = "year.duckdb"


def session_bounds(trading_date: date) -> tuple[datetime, datetime]:
    """返回（包含边界的）CST 会话开盘与收盘时间。"""
    return (
        datetime.combine(trading_date, SESSION_OPEN, tzinfo=CST),
        datetime.combine(trading_date, SESSION_CLOSE, tzinfo=CST),
    )


def announcement_close(announcement: date) -> datetime:
    """保守 PIT 规则：日级公告在收盘时方可可见。"""
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
    """解析覆盖请求区间的一系列 ``{root}/{year}/{filename}`` 文件。"""
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
    """解析覆盖请求区间的一系列 ``{root}/{year}/year.duckdb`` 文件。"""
    return resolve_year_files(root, YEAR_DB, start, end)


def read_rows(
    db_path: Path,
    table: str,
    columns: list[str] | None = None,
    where: str | None = None,
    params: list | None = None,
) -> list[dict]:
    """从 duckdb 文件读取单张表为 ``list[dict]``（不经 pandas）。"""
    if not db_path.is_file():
        raise FileNotFoundError(f"duckdb data file not found: {db_path}")
    projection = ", ".join(f'"{c}"' for c in columns) if columns else "*"
    sql = f'SELECT {projection} FROM "{table}"'
    if where:
        sql += f" WHERE {where}"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        result = con.execute(sql, params or [])
        names = [d[0] for d in result.description]
        return [dict(zip(names, row)) for row in result.fetchall()]
    finally:
        con.close()


def table_exists(db_path: Path, table: str) -> bool:
    """``table`` 是否存在于 ``db_path`` 处的 duckdb 文件中。"""
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
    """稳定数据版本：对数据源的（相对名称、大小、mtime）计算 md5。"""
    digest = hashlib.md5()
    for path in sorted(paths, key=lambda p: str(p)):
        stat = path.stat()
        digest.update(f"{path.name}|{stat.st_size}|{int(stat.st_mtime)}".encode())
    return "wp-" + digest.hexdigest()[:16]


def date_range_days(start: datetime | date | None, end: datetime | date | None) -> list[date]:
    """半开区间 [start, end) 请求窗口覆盖的交易日。

    ``end`` 为排除边界：午夜 end 会排除 end 当天本身。
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
    "read_rows",
    "resolve_year_dbs",
    "resolve_year_files",
    "session_bounds",
    "source_revision",
    "table_exists",
    "to_yyyymmdd",
]
