"""Tick CSV ingestion (legacy scope ``trade_data/tick``), polars-accelerated.

Ports the legacy ``import_tick_from_csv_fast`` pipeline: raw per-instrument
trade CSVs (GBK, one file per instrument per day, exported from the vendor's
zip dumps) are parsed with polars and landed per trading day at
``{root}/{year}/{MMDD}/{stock,tradable,index}.parquet``.

The output schemas are byte-for-byte compatible with the existing volume
(dtypes verified against real ``E:/ProgramData`` files):

- ``stock.parquet``    28 columns ``time, pr, vol, total_vol, amount,
  b1p..b5p, b1v..b5v, s1p..s5p, s1v..s5v, bs, code, flag``; prices are
  Float64, quantities/amount Int64, ``flag`` Int64.
- ``tradable.parquet`` same 28 columns; numerics Float64, ``flag`` Int32.
- ``index.parquet``    6 columns ``code, time, pr, vol, total_vol, amount``,
  numerics Float64.

Units stay vendor-native: ``vol``/``total_vol`` are raw quote-machine lots
increments/cumulative, ``amount`` is raw yuan — the lots->shares conversion
for equities lives in the ``market.trade``/``market.quote`` adapters.

``flag`` semantics: 0 = opening call auction (``total_vol == 0``), 1 =
continuous session, 2 = closing call auction (stocks only: 14:56:30-15:00:30
with ``vol == 0``).

Usage (library)::

    from data.downloader import TushareDownloader
    downloader = TushareDownloader(client=object(), root="E:/ProgramData")
    downloader.download("trade_data/tick", "20240102", "20240131",
                        options={"csv_root": "E:/2024"})

Usage (CLI)::

    python -m data.downloader.tick_import --input E:/2024 --month 202401
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Any, Optional

import polars as pl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# instrument-code routing tables (ported verbatim from the legacy importer)
# ---------------------------------------------------------------------------

# A 股代码前缀（导入到 stock.parquet）
STOCK_PREFIXES_SH = ("600", "601", "603", "605", "608", "688", "689")
STOCK_PREFIXES_SZ = ("000", "001", "002", "003", "300", "301")
ALL_STOCK_PREFIXES = STOCK_PREFIXES_SH + STOCK_PREFIXES_SZ

# ETF 代码前缀（导入到 tradable.parquet）
ETF_PREFIXES_SH = ("510", "512", "513", "515", "516", "517", "518", "560", "561", "562", "563", "588")
ETF_PREFIXES_SZ = ("159", "155")
ALL_ETF_PREFIXES = ETF_PREFIXES_SH + ETF_PREFIXES_SZ

# LOF 基金代码前缀（导入到 tradable.parquet）
LOF_PREFIXES_SH = ("501", "506", "508", "010", "018", "240")
LOF_PREFIXES_SZ = ("160", "161", "162", "163", "164", "165", "166", "167", "168", "169", "180")
ALL_LOF_PREFIXES = LOF_PREFIXES_SH + LOF_PREFIXES_SZ

# 可转债代码前缀（导入到 tradable.parquet）
CONVERTIBLE_BOND_PREFIXES_SH = ("110", "111", "113", "115", "118")
CONVERTIBLE_BOND_PREFIXES_SZ = ("123", "127", "128", "152")
ALL_CONVERTIBLE_BOND_PREFIXES = CONVERTIBLE_BOND_PREFIXES_SH + CONVERTIBLE_BOND_PREFIXES_SZ

# 公募 REITs 代码前缀（导入到 tradable.parquet）
REITS_PREFIXES_SH = ("019", "163", "175", "184", "185", "188")
REITS_PREFIXES_SZ = ("184",)
ALL_REITS_PREFIXES = REITS_PREFIXES_SH + REITS_PREFIXES_SZ

# 国债逆回购代码前缀（导入到 tradable.parquet）
REPO_PREFIXES_SH = ("204",)

ALL_TRADABLE_PREFIXES = (
    ALL_ETF_PREFIXES
    + ALL_LOF_PREFIXES
    + ALL_CONVERTIBLE_BOND_PREFIXES
    + ALL_REITS_PREFIXES
    + REPO_PREFIXES_SH
)

# 指数代码前缀（导入到 index.parquet）
INDEX_PREFIXES_SH = ("000",)  # 000xxx.SH
INDEX_PREFIXES_SZ = ("399",)  # 399xxx.SZ

# 不导入的代码前缀
EXCLUDED_PREFIXES_SH = ("900", "502", "751", "753", "755", "888")  # B股、分级基金、封闭式基金等
EXCLUDED_PREFIXES_SZ = ("200",)  # B股
ALL_EXCLUDED_PREFIXES = EXCLUDED_PREFIXES_SH + EXCLUDED_PREFIXES_SZ

# ---------------------------------------------------------------------------
# output schemas (verified against real E:/ProgramData files)
# ---------------------------------------------------------------------------

STOCK_COLUMNS = [
    "time", "pr", "vol", "total_vol", "amount",
    "b1p", "b1v", "b2p", "b2v", "b3p", "b3v", "b4p", "b4v", "b5p", "b5v",
    "s1p", "s1v", "s2p", "s2v", "s3p", "s3v", "s4p", "s4v", "s5p", "s5v",
    "bs", "code", "flag",
]
INDEX_COLUMNS = ["code", "time", "pr", "vol", "total_vol", "amount"]

# stock.parquet: prices Float64, quantities/amount Int64 (matches the volume)
STOCK_PRICE_COLUMNS = ("pr", "b1p", "b2p", "b3p", "b4p", "b5p", "s1p", "s2p", "s3p", "s4p", "s5p")
STOCK_QUANTITY_COLUMNS = (
    "vol", "total_vol", "amount",
    "b1v", "b2v", "b3v", "b4v", "b5v", "s1v", "s2v", "s3v", "s4v", "s5v",
)
BOOK_NUMERIC_COLUMNS = STOCK_PRICE_COLUMNS + STOCK_QUANTITY_COLUMNS

DATE_DIR_RE = re.compile(r"^\d{8}$")

CSV_COLUMN_MAP = {
    "时间": "time",
    "成交价": "pr",
    "成交量": "vol",
    "总量": "total_vol",
    "额": "amount",
    "B1价": "b1p", "B1量": "b1v",
    "B2价": "b2p", "B2量": "b2v",
    "B3价": "b3p", "B3量": "b3v",
    "B4价": "b4p", "B4量": "b4v",
    "B5价": "b5p", "B5量": "b5v",
    "S1价": "s1p", "S1量": "s1v",
    "S2价": "s2p", "S2量": "s2v",
    "S3价": "s3p", "S3量": "s3v",
    "S4价": "s4p", "S4量": "s4v",
    "S5价": "s5p", "S5量": "s5v",
    "BS": "bs",
}


def detect_exchange(dir_name: str) -> Optional[str]:
    """从目录名检测交易所后缀。"""
    dir_upper = dir_name.upper()
    if "SH" in dir_upper:
        return ".SH"
    if "SZ" in dir_upper:
        return ".SZ"
    return None


def parse_month_from_dir(dir_name: str) -> Optional[str]:
    """从目录名提取月份信息（YYYYMM 前缀）。"""
    match = re.match(r"(\d{6})", dir_name)
    return match.group(1) if match else None


def get_code_type(pure_code: str, exchange_suffix: str) -> str:
    """判断代码类型，决定导入哪个 parquet 文件。

    Returns:
        'stock', 'tradable', 'index', 或 'exclude'
    """
    if pure_code.startswith(ALL_EXCLUDED_PREFIXES):
        return "exclude"

    # 指数检查必须在 A 股之前（000 开头有歧义）
    if exchange_suffix == ".SH" and pure_code.startswith(INDEX_PREFIXES_SH):
        return "index"
    if exchange_suffix == ".SZ" and pure_code.startswith(INDEX_PREFIXES_SZ):
        return "index"

    if exchange_suffix == ".SH" and pure_code.startswith(STOCK_PREFIXES_SH):
        return "stock"
    if exchange_suffix == ".SZ" and pure_code.startswith(STOCK_PREFIXES_SZ):
        return "stock"

    if pure_code.startswith(ALL_ETF_PREFIXES):
        return "tradable"
    if pure_code.startswith(ALL_LOF_PREFIXES):
        return "tradable"
    if pure_code.startswith(ALL_CONVERTIBLE_BOND_PREFIXES):
        return "tradable"
    if pure_code.startswith(ALL_REITS_PREFIXES):
        return "tradable"
    if exchange_suffix == ".SH" and pure_code.startswith(REPO_PREFIXES_SH):
        return "tradable"

    return "exclude"


# ---------------------------------------------------------------------------
# CSV -> per-day frames
# ---------------------------------------------------------------------------


def import_single_csv(
    csv_path: Path,
    exchange_suffix: str,
    etf_only: bool = False,
) -> Optional[pl.DataFrame]:
    """Import one instrument CSV into a typed frame (None when skipped)."""
    pure_code = csv_path.stem.split("_")[0]
    code_type = get_code_type(pure_code, exchange_suffix)
    if code_type == "exclude":
        return None
    if etf_only and code_type != "tradable":
        return None

    code_with_suffix = (
        pure_code
        if pure_code.endswith((".SH", ".SZ"))
        else pure_code + exchange_suffix
    )

    try:
        df = pl.read_csv(csv_path, encoding="gbk")
    except Exception as e:
        logger.warning("[import] 读取失败 %s: %s", csv_path.name, e)
        return None
    if df.is_empty():
        return None

    df = df.rename({k: v for k, v in CSV_COLUMN_MAP.items() if k in df.columns})
    df = df.with_columns(pl.lit(code_with_suffix).cast(pl.String).alias("code"))

    if code_type == "index":
        return _finalize_index(df)
    return _finalize_book(df, code_type)


def _cast_f64(frame: pl.DataFrame, col: str) -> pl.DataFrame:
    return frame.with_columns(pl.col(col).cast(pl.Float64, strict=False).fill_null(0.0))


def _cast_i64(frame: pl.DataFrame, col: str) -> pl.DataFrame:
    return frame.with_columns(
        pl.col(col).cast(pl.Float64, strict=False).fill_null(0.0).cast(pl.Int64)
    )


def _finalize_index(frame: pl.DataFrame) -> pl.DataFrame:
    """指数数据：6 列、数值列 Float64（与真实 index.parquet 同构）。"""
    for col in ("pr", "vol", "total_vol", "amount"):
        if col in frame.columns:
            frame = _cast_f64(frame, col)
    return frame.select([c for c in INDEX_COLUMNS if c in frame.columns])


def _finalize_book(frame: pl.DataFrame, code_type: str) -> pl.DataFrame:
    """A 股/可交易标的：完整 28 列 + flag（dtype 按文件类型对齐真实卷）。"""
    for col in ("vol", "total_vol"):
        if col in frame.columns:
            # flag 判断需要数值化的量列
            frame = _cast_f64(frame, col)

    frame = frame.with_columns(_flag_expr(code_type).alias("flag"))

    if code_type == "stock":
        # stock.parquet 同构：价格 Float64、量/额 Int64、flag Int64
        for col in STOCK_PRICE_COLUMNS:
            if col in frame.columns:
                frame = _cast_f64(frame, col)
        for col in STOCK_QUANTITY_COLUMNS:
            if col in frame.columns:
                frame = _cast_i64(frame, col)
        frame = frame.with_columns(pl.col("flag").cast(pl.Int64))
    else:
        # tradable.parquet 同构：数值列 Float64、flag Int32
        for col in BOOK_NUMERIC_COLUMNS:
            if col in frame.columns:
                frame = _cast_f64(frame, col)
        frame = frame.with_columns(pl.col("flag").cast(pl.Int32))

    return frame.select([c for c in STOCK_COLUMNS if c in frame.columns])


def _flag_expr(code_type: str) -> pl.Expr:
    """flag=0 开盘竞价(total_vol==0)；flag=1 连续竞价；A股 14:56:30-15:00:30 且 vol==0 为收盘竞价。"""
    expr = (
        pl.when(pl.col("total_vol") == 0)
        .then(0)
        .otherwise(1)
    )
    if code_type == "stock":
        return (
            pl.when(pl.col("total_vol") == 0)
            .then(0)
            .when(
                (pl.col("time") >= "14:56:30")
                & (pl.col("time") <= "15:00:30")
                & (pl.col("vol") == 0)
            )
            .then(2)
            .otherwise(1)
        )
    return expr


# ---------------------------------------------------------------------------
# per-day persistence
# ---------------------------------------------------------------------------


def save_to_parquet(
    df: pl.DataFrame,
    output_path: str | Path,
    append: bool = False,
) -> None:
    """Save a frame to parquet (zstd); append mode dedupes on (code, time)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if append and output_path.exists():
        try:
            existing = pl.read_parquet(output_path)
            df = pl.concat([existing, df])
            df = df.unique(subset=["code", "time"], keep="last")
        except Exception as e:
            logger.warning("[save] 读取现有数据失败: %s", e)

    df.write_parquet(output_path, compression="zstd")


def import_date_range(
    csv_root: str | Path,
    output_root: str | Path,
    start: Optional[str] = None,
    end: Optional[str] = None,
    month: Optional[str] = None,
    append: bool = True,
    etf_only: bool = False,
    smart_route: bool = True,
) -> int:
    """Import tick CSVs under ``csv_root`` into the per-day parquet layout.

    Args:
        csv_root: directory holding the vendor month dirs (``.../SH/202401/20240102/*.csv``)
        output_root: raw parquet volume root
        start/end: inclusive YYYYMMDD filter for the scope-based entry point
        month: import a single YYYYMM month (``None`` + no start/end = first found month)
        append: merge into existing per-day files (dedupe on code+time)
        etf_only: only import ETF/fund instruments
        smart_route: route by instrument code (False: everything to stock.parquet)

    Returns:
        Number of CSV files imported.
    """
    csv_root = Path(csv_root)
    date_dirs = _find_date_dirs(csv_root, month)

    if not date_dirs:
        logger.error("[import] 未找到日期目录: %s (month=%s)", csv_root, month)
        return 0

    if start is not None:
        date_dirs = [d for d in date_dirs if d.name >= start]
    if end is not None:
        date_dirs = [d for d in date_dirs if d.name <= end]
    if not date_dirs:
        logger.error("[import] 区间 %s~%s 内无日期目录", start, end)
        return 0

    logger.info("[import] 找到 %d 个日期目录", len(date_dirs))

    total_imported = 0
    total_rows = {"stock": 0, "tradable": 0, "index": 0}

    for date_dir in date_dirs:
        date_str = date_dir.name
        csv_files = list(date_dir.glob("*.csv"))
        if not csv_files:
            continue

        exchange_suffix = detect_exchange(date_dir.parent.name) or detect_exchange(
            date_dir.parent.parent.name
        )
        if exchange_suffix is None:
            exchange_suffix = ".SH"

        logger.info(
            "[import] %s: 找到 %d 个 CSV 文件 (交易所: %s)",
            date_str, len(csv_files), exchange_suffix,
        )

        routed: dict[str, list[pl.DataFrame]] = {"stock": [], "tradable": [], "index": []}
        for csv_path in csv_files:
            df = import_single_csv(csv_path, exchange_suffix, etf_only=etf_only)
            if df is None or df.is_empty():
                continue
            pure_code = csv_path.stem.split("_")[0]
            if smart_route:
                code_type = get_code_type(pure_code, exchange_suffix)
                if code_type in routed:
                    routed[code_type].append(df)
            else:
                routed["stock"].append(df)

        output_dir = Path(output_root) / date_str[:4] / date_str[4:8]
        for code_type, frames in routed.items():
            if not frames:
                continue
            merged = pl.concat(frames)
            save_to_parquet(merged, output_dir / f"{code_type}.parquet", append=append)
            total_rows[code_type] += merged.height
            logger.info("[import] %s: %s.parquet 保存 %d 行", date_str, code_type, merged.height)

        total_imported += len(routed["stock"]) + len(routed["tradable"]) + len(routed["index"])

    logger.info(
        "[import] 完成: 导入 %d 个文件 (stock %d 行, tradable %d 行, index %d 行)",
        total_imported, total_rows["stock"], total_rows["tradable"], total_rows["index"],
    )
    return total_imported


def _find_date_dirs(csv_root: Path, month: Optional[str]) -> list[Path]:
    """Locate YYYYMMDD directories under the vendor month layout (two nesting depths).

    The month dir is the date directory's direct parent; when ``month`` is
    given only date dirs whose parent parses to that YYYYMM are returned.
    """
    for depth in (1, 2):
        found: list[Path] = []
        for parent in sorted(csv_root.iterdir()):
            if not parent.is_dir():
                continue
            if depth == 1:
                candidates = [
                    p for p in parent.iterdir()
                    if p.is_dir() and DATE_DIR_RE.match(p.name)
                ]
            else:
                candidates = [
                    p for mid in parent.iterdir() if mid.is_dir()
                    for p in mid.iterdir()
                    if p.is_dir() and DATE_DIR_RE.match(p.name)
                ]
            if month is not None:
                candidates = [
                    p for p in candidates
                    if parse_month_from_dir(p.parent.name) == month
                ]
            found.extend(sorted(candidates))
        if found:
            return found
    return []


def scan_available_months(input_dir: str | Path) -> list[str]:
    """扫描输入目录中可用的月份。"""
    input_dir = Path(input_dir)
    months: list[str] = []
    for d in input_dir.iterdir():
        if not d.is_dir():
            continue
        month = parse_month_from_dir(d.name)
        if month:
            months.append(month)
        else:
            for sub in d.iterdir():
                if sub.is_dir():
                    month = parse_month_from_dir(sub.name)
                    if month:
                        months.append(month)
                        break
    return sorted(set(months))


# ---------------------------------------------------------------------------
# registry adapters (legacy scope ``trade_data/tick``)
# ---------------------------------------------------------------------------


def fetch_tick_import(client: Any, start: str, end: str, *, csv_root: str | None = None, **options: Any) -> None:
    """Validate the CSV source; the actual work happens in ``write_tick_import``."""
    if csv_root is None:
        raise ValueError(
            "trade_data/tick ingests vendor CSV dumps, not a Tushare interface; "
            "pass options={'csv_root': <directory>} (see module docstring)"
        )
    return None


def write_tick_import(
    writer: Any,
    payload: None,
    *,
    start: str = "19000101",
    end: str = "29991231",
    csv_root: str | None = None,
    append: bool = True,
    etf_only: bool = False,
    smart_route: bool = True,
    **options: Any,
) -> list[Path]:
    """Run the CSV ingestion against the writer's volume root."""
    if csv_root is None:
        raise ValueError("trade_data/tick requires the 'csv_root' option")
    import_date_range(
        csv_root,
        writer.root,
        start=start,
        end=end,
        append=append,
        etf_only=etf_only,
        smart_route=smart_route,
    )
    # touched files are discovered by the caller from the volume if needed;
    # return the per-day files actually present in the written range
    root = Path(writer.root)
    return sorted(
        path
        for day_dir in root.glob("*/*")
        if day_dir.is_dir() and start <= f"{day_dir.parent.name}{day_dir.name}" <= end
        for path in day_dir.glob("*.parquet")
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[list] = None) -> None:
    """主入口（polars 加速版）。"""
    parser = argparse.ArgumentParser(description="将原始分笔 CSV 导入为 parquet（polars 加速版）")
    parser.add_argument("--input", "-i", required=True, help="输入目录")
    parser.add_argument("--month", default=None, help="导入指定月份 YYYYMM")
    parser.add_argument("--all", action="store_true", help="导入所有可用月份")
    parser.add_argument("--data-path", default="E:/ProgramData", help="输出数据根目录")
    parser.add_argument("--append", action="store_true", help="追加到现有 parquet")
    parser.add_argument("--etf-only", action="store_true", help="只导入 ETF 和基金")
    parser.add_argument("--no-smart-route", action="store_true", help="禁用智能路由")
    parser.add_argument("--dry-run", action="store_true", help="只扫描不导入")

    args = parser.parse_args(argv)

    input_dir = Path(args.input)
    if not input_dir.exists():
        logger.error("输入目录不存在: %s", input_dir)
        return

    available_months = scan_available_months(input_dir)
    if not available_months:
        logger.error("未找到可用月份目录")
        return

    print(f"可用月份: {available_months}")

    if args.dry_run:
        for month in available_months:
            print(f"  {month}")
        return

    if args.month:
        months_to_import = [args.month]
    elif args.all:
        months_to_import = available_months
    else:
        months_to_import = [available_months[0]]
        print(f"未指定月份，默认导入: {months_to_import[0]}")

    for month in months_to_import:
        print(f"\n{'=' * 60}")
        print(f"导入月份: {month}")
        print(f"{'=' * 60}")
        import_date_range(
            csv_root=input_dir,
            output_root=args.data_path,
            month=month,
            append=args.append,
            etf_only=args.etf_only,
            smart_route=not args.no_smart_route,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
