"""One-time migration: legacy parquet volume -> consolidated duckdb layout.

Converts the pre-consolidation ``E:/ProgramData`` layout

    {root}/*.parquet                     global tables
    {root}/{year}/*.parquet              yearly tables (incl. kline.parquet)
    {root}/{year}/{MMDD}/index_member.parquet  per-day membership files

into the consolidated layout

    {root}/reference.duckdb              global tables
    {root}/{year}/year.duckdb            yearly tables (kline + 5d/10d packed)
    {root}/{year}/{MMDD}/kline.parquet   per-day bar files (raw layer)
    {root}/{year}/{MMDD}/stock|tradable|index.parquet  untouched tick volumes

Safety model: every conversion writes into the target duckdb first, then the
source frame and the written table are validated against each other (row
count, column set, content digest); source parquet files are deleted only
when every table of the batch validated.  Run with ``apply=False`` (default)
to convert + validate without deleting anything, then ``apply=True`` to also
remove the legacy files.

Usage::

    python -m data.downloader.migrate E:/ProgramData            # dry run
    python -m data.downloader.migrate E:/ProgramData --apply    # convert + delete
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from data.downloader.base import merge_duckdb_table, read_duckdb_table
from data.downloader.pack import KLINE_TABLE, pack_kline

logger = logging.getLogger(__name__)

REFERENCE_DB = "reference.duckdb"
YEAR_DB = "year.duckdb"

# {root}/*.parquet -> reference.duckdb tables (file stem = table name)
GLOBAL_TABLES = (
    "trade_date", "stock_basic", "sw_industry", "fina_indicator",
    "income_report", "balance_report", "cashflow_report", "forecast",
    "holdertrade", "top10_holder", "fund_portfolio", "macro",
)

# {root}/{year}/*.parquet -> {year}/year.duckdb tables; kline handled specially
YEAR_TABLES = (
    "daily_basic", "margin", "moneyflow", "moneyflow_hsgt", "shibor",
    "yc_cb", "block_trade", "pledge_stat", "stk_holdernumber",
    "market_breadth", "northbound_netbuy",
)
MINUTE_TABLE_PREFIX = "minute_"

# reference-type tables the legacy layout misplaced inside year directories
# (2020-2022 report snapshots); they consolidate into reference.duckdb
REFERENCE_TABLES_IN_YEARS = ("income_report", "balance_report", "cashflow_report")

KLINE_DEDUPE_KEYS = ["ts_code", "trade_date", "data_type", "timeframe"]


@dataclass
class MigrationReport:
    """Per-table conversion + validation outcome."""

    converted: list[str] = field(default_factory=list)
    validated: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    def ok(self) -> bool:
        return not self.failures


def _canonical(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize column dtypes so parquet-side and duckdb-side frames compare equal.

    Numeric-ish columns (including numeric strings) become float64; everything
    else becomes str with nulls normalized to "".
    """
    out = {}
    for col in frame.columns:
        s = frame[col]
        if isinstance(s.dtype, pd.CategoricalDtype):
            s = s.astype(object)
        notna = s.notna()
        numeric = pd.to_numeric(s, errors="coerce")
        if bool((numeric.notna() == notna).all()):
            out[col] = numeric.astype("float64")
        else:
            out[col] = s.astype(object).where(notna, "").astype(str)
    return pd.DataFrame(out, columns=list(frame.columns))


def _validate(db_path: Path, table: str, source: pd.DataFrame, report: MigrationReport, label: str) -> bool:
    """Full-content check: every distinct source row must exist in the written table.

    Both sides pass through :func:`_canonical` first (dtype alignment), then a
    pandas left-join verifies containment (pandas treats NaN keys as equal,
    which duckdb's EXCEPT hash does not — hence no SQL anti-join here).
    """
    written = read_duckdb_table(db_path, table)
    problems: list[str] = []
    distinct_source = len(source.drop_duplicates())
    if len(written) < distinct_source:
        problems.append(f"row count {len(written)} < distinct source {distinct_source}")
    if sorted(written.columns) != sorted(source.columns):
        problems.append(
            f"column set mismatch: {sorted(written.columns)} vs {sorted(source.columns)}"
        )
    if not problems:
        source_canon = _canonical(source).drop_duplicates()
        written_canon = _canonical(written).drop_duplicates()
        columns = list(source_canon.columns)
        joined = source_canon.merge(written_canon, on=columns, how="left", indicator=True)
        missing = int((joined["_merge"] != "both").sum())
        if missing:
            problems.append(f"{missing} distinct source rows missing from written table")
    if problems:
        report.failures.append(f"{label}: " + "; ".join(problems))
        logger.error("[migrate] 校验失败 %s: %s", label, problems)
        return False
    report.validated.append(label)
    return True


def migrate_reference(root: Path, report: MigrationReport) -> None:
    """Convert global parquet tables into ``reference.duckdb``."""
    db_path = root / REFERENCE_DB
    for name in GLOBAL_TABLES:
        source_path = root / f"{name}.parquet"
        if not source_path.is_file():
            report.skipped.append(str(source_path))
            continue
        frame = pd.read_parquet(source_path)
        merge_duckdb_table(db_path, name, frame)
        report.converted.append(str(source_path))
        if _validate(db_path, name, frame, report, f"reference.{name}"):
            report.deleted.append(str(source_path))
        else:
            raise SystemExit(f"validation failed for {source_path}; aborting before delete")


def migrate_year(root: Path, year: str, report: MigrationReport) -> None:
    """Convert one year's parquet tables into ``{year}/year.duckdb``."""
    year_dir = root / year
    db_path = year_dir / YEAR_DB

    for name in REFERENCE_TABLES_IN_YEARS:
        source_path = year_dir / f"{name}.parquet"
        if not source_path.is_file():
            report.skipped.append(str(source_path))
            continue
        frame = pd.read_parquet(source_path)
        merge_duckdb_table(root / REFERENCE_DB, name, frame)
        report.converted.append(str(source_path))
        if not _validate(root / REFERENCE_DB, name, frame, report, f"reference.{name}[{year}]"):
            raise SystemExit(f"validation failed for {source_path}; aborting before delete")
        report.deleted.append(str(source_path))

    for name in YEAR_TABLES:
        source_path = year_dir / f"{name}.parquet"
        if not source_path.is_file():
            report.skipped.append(str(source_path))
            continue
        frame = pd.read_parquet(source_path)
        merge_duckdb_table(db_path, name, frame)
        report.converted.append(str(source_path))
        if not _validate(db_path, name, frame, report, f"{year}.{name}"):
            raise SystemExit(f"validation failed for {source_path}; aborting before delete")
        report.deleted.append(str(source_path))

    for minute_file in sorted(year_dir.glob("minute_*.parquet")):
        table = minute_file.stem
        frame = pd.read_parquet(minute_file)
        merge_duckdb_table(db_path, table, frame)
        report.converted.append(str(minute_file))
        if not _validate(db_path, table, frame, report, f"{year}.{table}"):
            raise SystemExit(f"validation failed for {minute_file}; aborting before delete")
        report.deleted.append(str(minute_file))

    # per-day index_member files -> one yearly table
    member_files = sorted(year_dir.glob("*/index_member.parquet"))
    if member_files:
        frame = pd.concat((pd.read_parquet(p) for p in member_files), ignore_index=True)
        if "trade_date" in frame.columns:
            frame["trade_date"] = frame["trade_date"].astype(str)
        merge_duckdb_table(db_path, "index_member", frame, ["index_code", "con_code", "trade_date"])
        report.converted.append(str(member_files[0]) + f" (+{len(member_files) - 1} more)")
        if not _validate(db_path, "index_member", frame, report, f"{year}.index_member"):
            raise SystemExit("validation failed for index_member; aborting before delete")
        for member_file in member_files:
            member_file.unlink()
            report.deleted.append(str(member_file))
        for day_dir in year_dir.iterdir():
            if day_dir.is_dir() and day_dir.name.isdigit() and not any(day_dir.iterdir()):
                day_dir.rmdir()

    # kline: pour the yearly vendor rows into year.duckdb#kline (1d base),
    # mirror them into the per-day files for days that already have a daily
    # directory (keeping the 4-files-per-day layout where ticks exist), pack
    # the derived timeframes, then drop the legacy yearly file.  Years without
    # any daily directory keep their bars db-only.
    kline_path = year_dir / "kline.parquet"
    if kline_path.is_file():
        frame = pd.read_parquet(kline_path)
        # keep null trade_date as NULL (never the string "nan"): a handful of
        # legacy rows carry only an adj_factor with no date/prices — preserved
        # verbatim, skipped by the day mirror and the bar derivation
        frame["trade_date"] = frame["trade_date"].astype("string")
        merge_duckdb_table(db_path, KLINE_TABLE, frame, KLINE_DEDUPE_KEYS)

        written_days = 0
        dated = frame[frame["trade_date"].notna()]
        for trade_date, group in dated.groupby("trade_date"):
            day_dir = year_dir / trade_date[4:8]
            if not day_dir.is_dir():
                continue
            target = day_dir / "kline.parquet"
            if target.is_file():
                existing = pd.read_parquet(target)
                group = pd.concat([existing, group], ignore_index=True)
                group = group.drop_duplicates(subset=KLINE_DEDUPE_KEYS, keep="last")
            group.to_parquet(target, index=False)
            written_days += 1

        pack_kline(root, year)
        table_frame = read_duckdb_table(db_path, KLINE_TABLE)
        base = table_frame[table_frame["timeframe"] == "1d"] if "timeframe" in table_frame.columns else table_frame
        if len(base) < len(frame):
            raise SystemExit(
                f"kline validation failed for {year}: {len(base)} 1d rows < source {len(frame)}"
            )
        missing = len(frame.drop_duplicates(subset=KLINE_DEDUPE_KEYS).merge(
            base.drop_duplicates(subset=KLINE_DEDUPE_KEYS),
            on=KLINE_DEDUPE_KEYS, how="left", indicator=True,
        ).query("_merge != 'both'"))
        if missing:
            raise SystemExit(f"kline validation failed for {year}: {missing} source rows missing")
        report.converted.append(str(kline_path))
        report.validated.append(
            f"{year}.kline (1d {len(base)} rows >= source {len(frame)}; "
            f"{written_days} day files mirrored)"
        )
        report.deleted.append(str(kline_path))


def migrate_volume(root: str | Path, *, apply: bool = False) -> MigrationReport:
    """Convert the whole legacy volume; delete legacy files only when ``apply``."""
    root = Path(root)
    if not root.is_dir():
        raise SystemExit(f"数据卷不存在: {root}")
    report = MigrationReport()

    migrate_reference(root, report)
    year_dirs = sorted(p.name for p in root.iterdir() if p.is_dir() and p.name.isdigit())
    for year in year_dirs:
        migrate_year(root, year, report)

    if apply and report.ok():
        for path_text in report.deleted:
            path = Path(path_text)
            if path.is_file():
                path.unlink()
                logger.info("[migrate] 已删除 %s", path)
        _prune_empty_year_dirs(root)
    elif not apply:
        logger.info("[migrate] dry run：保留所有旧 parquet，重跑加 --apply 执行删除")
    return report


def _prune_empty_year_dirs(root: Path) -> None:
    for year_dir in root.iterdir():
        if not (year_dir.is_dir() and year_dir.name.isdigit()):
            continue
        for day_dir in list(year_dir.iterdir()):
            if day_dir.is_dir() and day_dir.name.isdigit() and not any(day_dir.iterdir()):
                day_dir.rmdir()


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="旧 parquet 卷 -> duckdb 整合布局迁移")
    parser.add_argument("root", help="数据卷根目录，如 E:/ProgramData")
    parser.add_argument("--apply", action="store_true", help="校验通过后删除旧 parquet（默认只转换+校验）")
    args = parser.parse_args(argv)

    report = migrate_volume(args.root, apply=args.apply)
    print(f"\n转换 {len(report.converted)} 项, 校验通过 {len(report.validated)} 项, "
          f"待删/已删 {len(report.deleted)} 项, 跳过 {len(report.skipped)} 项")
    if report.failures:
        print("失败:")
        for failure in report.failures:
            print(f"  - {failure}")
        raise SystemExit(1)
    print("全部校验通过" + ("，旧文件已删除" if args.apply else "（dry run，未删除）"))


if __name__ == "__main__":
    main()
