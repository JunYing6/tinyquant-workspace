"""数据域下载器协议与原始卷写入器。

下载器是数据域的“写”端：拉取原始厂商数据帧，并按
``data.adapters.workspace_data`` 读取的确切物理布局落盘（该布局由每个
``MigrationMapping`` 声明）。厂商字段在此保持厂商原生；所有语义转换都在适配器中完成。

    vendor API -> Downloader -> raw volume -> Adapter -> DataGateway

写入器产出的物理布局：

- ``{root}/reference.duckdb``      每个非按日期切分表一张表
- ``{root}/{year}/year.duckdb``    每个按日期切分的年度表一张表
- ``{root}/{year}/{MMDD}/*.parquet``  按日文件（分笔成交量、日线）

模板将目标声明为 ``{root}/reference.duckdb#trade_date`` 或
``{year}/year.duckdb#daily_basic``（``#`` 后为表名）；parquet 模板
保留 ``{year}``/``{MMDD}`` 路径形式。重复写入会合并进现有表/文件；
``dedup_keys`` 选定业务键（keep="last"，因此重复下载会刷新既有行），
默认则按整行去重。

本模块还承载所有 scope 拉取器依赖的公共设施：月份迭代、
指数退避重试与 offset 分页（全部移植自旧版工作区下载器基建）。
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
    """反向查找：旧版 scope（如 ``trade_data/daily``）-> 数据集名称。"""
    return _DATASET_BY_SCOPE.get(scope)


@runtime_checkable
class Downloader(Protocol):
    """返回原始（厂商原生）数据帧的厂商拉取器。"""

    def fetch(self, scope: str, start: str, end: str) -> pd.DataFrame:
        """拉取 ``scope`` 在闭区间 YYYYMMDD 范围内的行。"""
        ...


# ---------------------------------------------------------------------------
# 公共设施（移植自旧版下载器基建）
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
    """调用厂商 API，带指数退避重试；重试耗尽时返回 ``None``。"""
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
    """Offset 分页拉取：递增 ``offset``，直到厂商返回不足以满页的数据为止。"""
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
    """产出覆盖闭区间 YYYYMMDD 范围的 ``YYYYMM`` 字符串。"""
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
    """将月份 ``ym``（``YYYYMM``）按 ``end_date`` 裁剪；返回 (start, end) 的 YYYYMMDD。"""
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
    """闭区间内的报告期截止日期（0331/0630/0930/1231）。"""
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
    """供 VIP 周期类 API 使用的报告期（``YYYYMMDD`` 季度末），以字符串形式返回。"""
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
# duckdb 表读写
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
    """从 duckdb 文件读取一张表到 pandas 数据帧。"""
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
    """将 ``frame`` 合并进 ``table``（按业务键去重，保留最后一条）；返回行数。

    每次调用整个表原子重写：读取已有行、拼接并去重，然后执行
    ``CREATE OR REPLACE TABLE``。连接按次打开与关闭，因此读者仅在
    重写期间被阻塞。
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
    """一个已解析的物理写入目标。"""

    kind: str  # 取值："parquet" | "duckdb"
    scoped: str  # 取值："year" | "root"
    template: str  # 原始模板，如 "{year}/year.duckdb#kline"
    per_day: bool  # 仅 parquet：{MMDD} 路径形式
    table: str | None  # 仅 duckdb


def parse_target(template: str) -> WriteTarget:
    """将写模板字符串解析为 kind/scope/table 各部分。"""
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
# 原始卷写入器
# ---------------------------------------------------------------------------


class RawVolumeWriter:
    """将原始数据帧落盘到 ``root`` 下的旧版工作区布局。

    目标路径/表从数据集的 ``MigrationMapping`` 解析（先取 ``write_targets``，
    再取 ``physical_sources``），因此写入器与读者在结构上保持一致：

    - ``{root}/reference.duckdb#trade_date``   全局快照表
    - ``{year}/year.duckdb#daily_basic``       按日期切分的年度表
    - ``{year}/{MMDD}/kline.parquet``          按交易日分组的行

    重复写入会合并进现有表/文件；``dedup_keys`` 选定业务键（keep="last"，
    因此重复下载会刷新既有行），默认则按整行去重。
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
        """为 ``scope`` 写入一个原始数据帧，并返回所受影响的文件路径。"""
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

    # -- 内部方法 ------------------------------------------------------------

    @staticmethod
    def _write_template(dataset: str, mapping: Any) -> str | None:
        candidates = tuple(mapping.write_targets) or tuple(mapping.physical_sources)
        # 优先按日 parquet，再按年，最后 root 级
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
