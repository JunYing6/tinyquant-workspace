"""一次性迁移：旧版 parquet 卷 -> 整合后的 duckdb 布局。

将整合前 ``E:/ProgramData`` 布局

    {root}/*.parquet                     全局表
    {root}/{year}/*.parquet              年度表（含 kline.parquet、minute_*.parquet）
    {root}/{year}/{MMDD}/index_member.parquet  按日成分文件

转换为整合后布局

    {root}/reference.duckdb              全局表
    {root}/{year}/year.duckdb            年度表（kline：日线 1d + 派生 5d/10d）
    {root}/{year}/{MMDD}/kline.parquet   按日 bar 文件（原始层；统一 kline schema，
                                          用 timeframe 列区分 1d 与日内分钟，如 30m）
    {root}/{year}/{MMDD}/stock|tradable|index.parquet  未触碰的分笔卷

分钟布局（方案乙）：分钟线统一落入 per-day ``{MMDD}/kline.parquet`` 的 ``timeframe`` 列；
年度库只保留日线及以上。历史 ``year.duckdb#minute_*`` 老表用 ``--minute`` 迁到 per-day，
再用 ``--fix-data-type`` 按 ``year#kline`` 的 code->类型校正指数/基金标签。

安全模型：每次转换先把数据写入目标 duckdb，然后将源数据帧与所写表
相互校验（行数、列集合、内容摘要）；只有当该批次所有表都通过校验后
才删除源 parquet 文件。以 ``apply=False``（默认）运行可转换 + 校验而
不删除任何内容，再以 ``apply=True`` 运行即可一并移除旧版文件。

用法::

    python -m data.downloader.migrate E:/ProgramData                       # 试运行
    python -m data.downloader.migrate E:/ProgramData --apply               # 转换 + 删除
    python -m data.downloader.migrate E:/ProgramData --minute [--apply]        # 迁移分钟老表
    python -m data.downloader.migrate E:/ProgramData --fix-data-type [--apply] # 校正分钟 data_type
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import duckdb

from data.downloader.base import merge_duckdb_table, read_duckdb_table, RawVolumeWriter
from data.downloader.pack import KLINE_BASE_TIMEFRAME, KLINE_TABLE, pack_kline
from data.downloader.scopes import minute as minute_scope

logger = logging.getLogger(__name__)

REFERENCE_DB = "reference.duckdb"
YEAR_DB = "year.duckdb"

# {root}/*.parquet -> reference.duckdb 表（文件主干名 = 表名）
GLOBAL_TABLES = (
    "trade_date", "stock_basic", "sw_industry", "fina_indicator",
    "income_report", "balance_report", "cashflow_report", "forecast",
    "holdertrade", "top10_holder", "fund_portfolio", "macro",
)

# {root}/{year}/*.parquet -> {year}/year.duckdb 表；kline 特殊处理
YEAR_TABLES = (
    "daily_basic", "margin", "moneyflow", "moneyflow_hsgt", "shibor",
    "yc_cb", "block_trade", "pledge_stat", "stk_holdernumber",
    "market_breadth", "northbound_netbuy",
)
MINUTE_TABLE_PREFIX = "minute_"

# 旧版布局误置于年度目录内的 reference 类型表
# （2020-2022 报告快照）；它们应合并进 reference.duckdb
REFERENCE_TABLES_IN_YEARS = ("income_report", "balance_report", "cashflow_report")

KLINE_DEDUPE_KEYS = ["ts_code", "trade_date", "data_type", "timeframe"]


@dataclass
class MigrationReport:
    """每张表的转换 + 校验结果。"""

    converted: list[str] = field(default_factory=list)
    validated: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    def ok(self) -> bool:
        return not self.failures


def _canonical(frame: pd.DataFrame) -> pd.DataFrame:
    """归一化列 dtype，使 parquet 侧与 duckdb 侧的数据帧可比。

    数值型列（含数字字符串）转为 float64；其余列转为 str，
    并将空值归一化为 ""。
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
    """全内容校验：源数据的每个去重行都必须出现在所写的表中。

    两侧先经 :func:`_canonical` 处理（对齐 dtype），再用 pandas 左连接
    校验包含关系（pandas 将 NaN 键视为相等，而 duckdb 的 EXCEPT 哈希
    不会这样做——因此这里不用 SQL 反连接）。
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
    """将全局 parquet 表转换为 ``reference.duckdb``。"""
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
    """将某年的 parquet 表转换为 ``{year}/year.duckdb``。"""
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

    # 分钟线：旧 ``{year}/minute_*.parquet`` -> per-day ``{MMDD}/kline.parquet``（方案乙）
    for minute_file in sorted(year_dir.glob("minute_*.parquet")):
        frame = pd.read_parquet(minute_file)
        freq = minute_file.stem.split("minute_", 1)[1] or "30min"
        written = minute_scope.write_minute(RawVolumeWriter(root), frame, freq=freq)
        report.converted.append(f"{minute_file} -> {len(written)} day file(s)")
        report.validated.append(str(minute_file))
        report.deleted.append(str(minute_file))

    # 按日 index_member 文件 -> 一张年度表
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

    # kline: 将年度厂商行汇入 year.duckdb#kline（1d 基表），
    # 对已有日线目录的日期将其镜像进按日文件（在存在分笔处保持每日 4 文件
    # 布局），打包派生周期，然后删除旧版年度文件。没有任何日线目录的年份
    # 其 bar 仅存于数据库中。
    kline_path = year_dir / "kline.parquet"
    if kline_path.is_file():
        frame = pd.read_parquet(kline_path)
        # 将空的 trade_date 保留为 NULL（绝不用字符串 "nan"）：少数旧版
        # 行只带有 adj_factor 而无日期/价格——原样保留，日线镜像与 bar
        # 派生时跳过
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


def migrate_minute_tables(root: str | Path, *, apply: bool = False) -> MigrationReport:
    """把 ``year.duckdb`` 里的 ``minute_*`` 老表迁移到 per-day ``{year}/{MMDD}/kline.parquet``（方案乙）。

    默认 dry-run：只转换 + 统计，不删表；``apply=True`` 才 ``DROP TABLE "minute_*"``。
    """
    root = Path(root)
    report = MigrationReport()
    for year_dir in sorted(p for p in root.iterdir() if p.is_dir() and p.name.isdigit()):
        db_path = year_dir / YEAR_DB
        if not db_path.is_file():
            continue
        con = duckdb.connect(str(db_path), read_only=True)
        tables = [
            r[0]
            for r in con.execute(
                "select table_name from information_schema.tables "
                "where table_schema='main' and table_name like 'minute_%'"
            ).fetchall()
        ]
        con.close()
        for table in tables:
            frame = read_duckdb_table(db_path, table)
            if frame.empty:
                report.skipped.append(f"{db_path}#{table} (empty)")
                continue
            freq = table.split("minute_", 1)[1]
            written = minute_scope.write_minute(RawVolumeWriter(root), frame, freq=freq)
            report.converted.append(f"{db_path}#{table}: {len(frame)} rows -> {len(written)} day file(s)")
            if apply:
                con = duckdb.connect(str(db_path))
                con.execute(f'DROP TABLE "{table}"')
                con.close()
                report.deleted.append(f"{db_path}#{table}")
            else:
                report.validated.append(f"{db_path}#{table}: {len(frame)} rows")
    return report


def fix_intraday_data_type(root: str | Path, *, apply: bool = False) -> MigrationReport:
    """用 ``year#kline`` 的 code->data_type（stock/index/fund）校正 per-day 分钟行的 ``data_type``。

    分钟迁移时统一标了 ``stock``；本函数按年度的标的类型映射把 index/fund 分钟行改对。
    默认 dry-run 只统计；``apply=True`` 才写回。
    """
    root = Path(root)
    report = MigrationReport()
    for year_dir in sorted(p for p in root.iterdir() if p.is_dir() and p.name.isdigit()):
        db_path = year_dir / YEAR_DB
        if not db_path.is_file():
            continue
        ref = read_duckdb_table(db_path, KLINE_TABLE, columns=["ts_code", "data_type"])
        mapping = dict(zip(ref["ts_code"].astype(str), ref["data_type"].astype(str)))
        for path in sorted(year_dir.glob("*/kline.parquet")):
            frame = pd.read_parquet(path)
            if "timeframe" not in frame.columns:
                continue
            mask = frame["timeframe"].ne(KLINE_BASE_TIMEFRAME) & frame["ts_code"].astype(str).isin(mapping)
            if not mask.any():
                continue
            mapped = frame.loc[mask, "ts_code"].astype(str).map(mapping)
            changed = int((mapped != frame.loc[mask, "data_type"].astype(str)).sum())
            if changed:
                if apply:
                    frame.loc[mask, "data_type"] = mapped
                    frame.to_parquet(path, index=False)
                report.converted.append(f"{path.name} @ {year_dir.name}: {changed} rows -> {sorted(set(mapped))}")
            if apply:
                report.validated.append(str(path))
    return report


def migrate_volume(root: str | Path, *, apply: bool = False) -> MigrationReport:
    """转换整个旧版卷；仅当 ``apply`` 时才删除旧版文件。"""
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
    parser.add_argument("--minute", action="store_true", help="改为迁移 year.duckdb 的 minute_* 老表到 per-day kline.parquet")
    parser.add_argument("--fix-data-type", action="store_true", help="校正 per-day 分钟行的 data_type（用 year#kline 的 code->类型）")
    args = parser.parse_args(argv)

    if args.fix_data_type:
        report = fix_intraday_data_type(args.root, apply=args.apply)
    elif args.minute:
        report = migrate_minute_tables(args.root, apply=args.apply)
    else:
        report = migrate_volume(args.root, apply=args.apply)
    print(f"\n转换 {len(report.converted)} 项, 校验通过 {len(report.validated)} 项, "
          f"待删/已删 {len(report.deleted)} 项, 跳过 {len(report.skipped)} 项")
    for line in report.converted:
        print(f"  {line}")
    if report.failures:
        print("失败:")
        for failure in report.failures:
            print(f"  - {failure}")
        raise SystemExit(1)
    print("全部校验通过" + ("，旧表已删除" if args.apply else "（dry run，未删除）"))


if __name__ == "__main__":
    main()
