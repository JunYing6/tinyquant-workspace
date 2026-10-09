"""基于整合后工作区数据卷的历史与日历适配器。

``WorkspaceParquetAdapter`` 提供一期表/bar 数据集：

- ``market.bar`` / ``index.bar`` / ``fund.bar``（按年 ``year.duckdb#kline``，
  由 ``data_type`` 列分派；1d 与多日周期）
- ``market.daily_metric``（``year.duckdb#daily_basic``）
- ``instrument.master``（``reference.duckdb#stock_basic``）
- ``industry.membership``（``reference.duckdb#sw_industry``）

``WorkspaceCalendarAdapter`` 从 ``reference.duckdb#trade_date`` 提供
``calendar.session``。

所有转换遵循 ``adapters/workspace_data/mappings.py``。
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from tools.data import (
    AdapterDescriptor,
    Bar,
    CalendarBatch,
    CalendarRequest,
    DataBatch,
    DataProvenance,
    DataRequest,
    DatasetCapability,
    QualityReport,
    QualityWarning,
    Session,
    TradingPhase,
    default_catalog,
)

from data.adapters.workspace_data.reader import (
    ASSET_TYPE_MAP,
    CST,
    MARKET,
    REFERENCE_DB,
    STATUS_MAP,
    announcement_close,
    date_range_days,
    parse_yyyymmdd,
    read_table,
    resolve_year_dbs,
    session_bounds,
    source_revision,
    to_yyyymmdd,
)

ADAPTER_NAME = "workspace-bar"
TABLE_ADAPTER_NAME = "workspace-table"
CALENDAR_NAME = "workspace-calendar"
SCHEMA_VERSION = "1"
SOURCE = "workspace-parquet"

_BAR_ALIASES = {"ts_code": "instrument_id", "trade_date": "trading_date", "vol": "volume", "amount": "turnover"}
_BAR_CONVERSIONS = {"volume": 100.0, "turnover": 1000.0}  # 手 -> 股，千元 -> 元
_INDEX_VOLUME_FACTOR = 1.0  # 指数量保留数据源原生单位（参见 mappings）


def _intraday_delta(freq: str) -> timedelta:
    unit = freq[-1]
    count = int(freq[:-1] or 1)
    return timedelta(minutes=count * (60 if unit == "h" else 1))


_METRIC_CONVERSIONS = {
    "total_mv": 10000.0,
    "circ_mv": 10000.0,
    "total_share": 10000.0,
    "float_share": 10000.0,
    "free_share": 10000.0,
}


def _capability(
    dataset: str,
    *,
    frequencies: tuple[str, ...],
    modes: tuple[str, ...] = ("historical",),
    point_in_time: bool | None = None,
) -> DatasetCapability:
    definition = default_catalog().get(dataset)
    # 策略请求始终携带注入的 as_of（pipeline），因此服务此类请求的适配器
    # 必须声明 PIT 能力并遵守该约束。
    pit = definition.point_in_time if point_in_time is None else point_in_time
    return DatasetCapability(
        dataset=dataset,
        modes=modes,
        asset_types=frozenset(definition.asset_types),
        frequencies=frequencies,
        fields=tuple(definition.fields),
        point_in_time=pit,
    )


def _batch(
    adapter_name: str,
    revision: str,
    request: Any,
    dataset: str,
    records: tuple[Any, ...],
    schema_version: str = SCHEMA_VERSION,
    warnings: tuple[QualityWarning, ...] = (),
) -> DataBatch:
    return DataBatch(
        request_id=getattr(request, "correlation_id", None) or "",
        dataset=dataset,
        schema_version=schema_version,
        correlation_id=getattr(request, "correlation_id", None),
        records=records,
        complete=True,
        next_cursor=None,
        provenance=DataProvenance(
            adapter_name=adapter_name,
            source_revision=revision,
            request_fingerprint=getattr(request, "delivery_key", None) or "workspace",
            read_at=datetime.now(tz=CST),
        ),
        quality=QualityReport(
            status="warning" if warnings else "ok",
            warnings=warnings,
            checked_count=len(records),
        ),
    )


class WorkspaceCalendarAdapter:
    """从 ``reference.duckdb#trade_date`` 提供 ``calendar.session``。"""

    def __init__(self, data_root: str | Path, market: str = MARKET) -> None:
        self._root = Path(data_root)
        self._market = market
        self.descriptor = AdapterDescriptor(
            name=CALENDAR_NAME,
            datasets={"calendar.session": _capability("calendar.session", frequencies=(), modes=("calendar",))},
            historical_modes=(),
            realtime_modes=(),
            supports_point_in_time=False,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset({"market"}),
            schema_versions=(SCHEMA_VERSION,),
        )

    def sessions(self, request: CalendarRequest) -> CalendarBatch:
        frame = read_table(self._root / REFERENCE_DB, "trade_date")
        open_days = {
            parse_yyyymmdd(v)
            for v, flag in zip(frame["cal_date"], frame["is_open"])
            if int(flag) == 1
        }
        days: list[date] = []
        current = request.start
        while current <= request.end:
            if current in open_days or request.include_closed:
                days.append(current)
            current = date.fromordinal(current.toordinal() + 1)
        records = []
        for day in days:
            if day in open_days:
                start, end = session_bounds(day)
                phases = (TradingPhase(name="regular", start=start, end=end, accepts_trades=True, accepts_quotes=True),)
            else:
                midnight = datetime.combine(day, time(0, 0), tzinfo=CST)
                phases = (TradingPhase(name="closed", start=midnight, end=midnight, accepts_trades=False, accepts_quotes=False),)
            records.append(Session(market=request.market, trading_date=day, timezone="Asia/Shanghai", phases=phases))
        batch = _batch(CALENDAR_NAME, "trade_date-static", request, "calendar.session", tuple(records))
        return batch


class WorkspaceBarAdapter:
    """提供 bar 系列（market.bar / index.bar / fund.bar）。非 PIT。"""

    DATASETS = ("market.bar", "index.bar", "fund.bar")

    def __init__(self, data_root: str | Path, market: str = MARKET) -> None:
        self._root = Path(data_root)
        self._market = market
        frequencies = ("1d", "30m")
        self._open_days_cache: list[date] | None = None
        self.descriptor = AdapterDescriptor(
            name=ADAPTER_NAME,
            datasets={
                "market.bar": _capability("market.bar", frequencies=frequencies, point_in_time=True),
                "index.bar": _capability("index.bar", frequencies=frequencies, point_in_time=True),
                "fund.bar": _capability("fund.bar", frequencies=frequencies, point_in_time=True),
            },
            historical_modes=("historical",),
            realtime_modes=(),
            supports_point_in_time=True,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset({"equity", "index", "fund"}),
            schema_versions=(SCHEMA_VERSION,),
        )

    def read(self, request: DataRequest) -> DataBatch:
        if request.dataset in ("market.bar", "index.bar", "fund.bar"):
            return self._read_bars(request)
        raise ValueError(f"workspace-bar adapter does not serve {request.dataset!r}")

    def iter(self, request: DataRequest, chunk_size: int = 10_000):
        yield self.read(request)

    def _resolve_range(self, request: DataRequest) -> tuple[date | None, date | None]:
        """解析起止日期，通过日历把 anchor + session_window 换算为具体区间。"""
        if request.start is not None or request.end is not None:
            return request.start, request.end
        anchor = request.anchor
        if anchor is None:
            return None, None
        anchor_d = anchor.date() if isinstance(anchor, datetime) else anchor
        window = request.session_window
        if window is None:
            return anchor_d, anchor_d
        open_days = self._open_days()
        if not open_days:
            return anchor_d, anchor_d
        import bisect

        idx = bisect.bisect_right(open_days, anchor_d) - 1
        if idx < 0:
            return anchor_d, anchor_d
        before, after = window
        start_idx = max(idx + before, 0)
        end_idx = min(idx + after, len(open_days) - 1)
        if start_idx > end_idx:
            return anchor_d, anchor_d
        return open_days[start_idx], open_days[end_idx]

    def _open_days(self) -> list[date]:
        if self._open_days_cache is None:
            frame = read_table(self._root / REFERENCE_DB, "trade_date")
            days = {
                parse_yyyymmdd(v)
                for v, flag in zip(frame["cal_date"], frame["is_open"])
                if int(flag) == 1 and parse_yyyymmdd(v) is not None
            }
            self._open_days_cache = sorted(days)
        return self._open_days_cache

    # -- bar 系列 -----------------------------------------------------------

    def _read_bars(self, request: DataRequest) -> DataBatch:
        view = request.dataset
        wanted_asset = {"market.bar": None, "index.bar": "index", "fund.bar": "fund"}[view]
        freq = request.frequency or "1d"
        if freq and freq[-1] in ("m", "h"):
            return self._read_intraday_bars(request, view, freq)
        start_d, end_d = self._resolve_range(request)
        files = resolve_year_dbs(self._root, start_d, end_d)
        revision = source_revision(files) if files else "wp-empty"
        if not files:
            return _batch(ADAPTER_NAME, revision, request, view, ())
        records: list[Bar] = []
        dropped = 0
        start_s = to_yyyymmdd(start_d) if start_d is not None else None
        end_s = to_yyyymmdd(end_d) if end_d is not None else None
        freq = request.frequency or "1d"
        for db_path in files:
            # 下推 instruments/日期/周期到 DuckDB，避免全表读入（Python 侧保留同样过滤作一致性保障）
            conditions = ["timeframe = ?"]
            params: list = [freq]
            if request.instruments is not None and len(request.instruments) > 0:
                placeholders = ", ".join("?" for _ in request.instruments)
                conditions.append(f"ts_code IN ({placeholders})")
                params.extend(request.instruments)
            if start_s is not None:
                conditions.append("trade_date >= ?")
                params.append(start_s)
            if end_s is not None:
                conditions.append("trade_date <= ?")
                params.append(end_s)
            frame = read_table(db_path, "kline", where=" AND ".join(conditions), params=params).rename(columns=_BAR_ALIASES)
            # 打包的 kline 表包含多日周期（5d/10d）；
            # 除非另行指定，本适配器仅提供日线 bar 视图
            frame = frame[frame["timeframe"] == freq]
            if wanted_asset is not None:
                frame = frame[frame["data_type"].map(ASSET_TYPE_MAP) == wanted_asset]
            if request.instruments is not None:
                frame = frame[frame["instrument_id"].isin(request.instruments)]
            # 日线 bar 采用区间重叠语义（与发布版参考适配器一致）：引擎请求
            # 某一天的 [session.open, session.close]，因此在 trading_date 上
            # 两个边界均为包含（inclusive）。
            if start_s is not None:
                frame = frame[frame["trading_date"] >= start_s]
            if end_s is not None:
                frame = frame[frame["trading_date"] <= end_s]
            before = len(frame)
            # ETF 同时以 stock 与 fund 两种 data_type 存储；market.bar 主键
            # 不含 asset_type，因此在边界处去重。
            frame = frame.drop_duplicates(subset=["instrument_id", "trading_date", "timeframe"], keep="first")
            dropped += before - len(frame)
            for row in frame.to_dict("records"):
                trading_date = parse_yyyymmdd(row.get("trading_date"))
                if trading_date is None:
                    continue
                prices = [row.get(c) for c in ("open", "high", "low", "close")]
                if any(v is None or v != v for v in prices):
                    continue  # 停牌/无成交的行不含 OHLC 事实
                data_type = str(row.get("data_type"))
                asset_type = ASSET_TYPE_MAP.get(data_type, data_type)
                volume_factor = _INDEX_VOLUME_FACTOR if asset_type == "index" else _BAR_CONVERSIONS["volume"]
                interval_start, interval_end = session_bounds(trading_date)
                # BAR_PIT：行情数据无公告延迟，快速回放以决策时刻（开盘）的
                # as_of 请求当前会话的 bar——可见性由请求的会话边界界定，
                # 而非 available_at。available_at 保留在收盘时刻用于溯源。
                raw_volume = row.get("volume")
                raw_turnover = row.get("turnover")
                records.append(
                    Bar(
                        schema_version=SCHEMA_VERSION,
                        event_id=None,
                        instrument_id=str(row["instrument_id"]),
                        asset_type=asset_type,
                        effective_time=interval_end,
                        event_time=interval_end,
                        available_at=interval_end,
                        trading_date=trading_date,
                        source=SOURCE,
                        quality="valid",
                        metadata={},
                        frequency=str(row.get("timeframe", "1d")),
                        interval_start=interval_start,
                        interval_end=interval_end,
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=float(raw_volume) * volume_factor if raw_volume == raw_volume else 0.0,
                        turnover=float(raw_turnover) * _BAR_CONVERSIONS["turnover"] if raw_turnover == raw_turnover else 0.0,
                        is_complete=True,
                        price_basis="raw",
                    )
                )
        records.sort(key=lambda bar: (bar.instrument_id, bar.interval_end))
        warnings: tuple[QualityWarning, ...] = ()
        if dropped:
            warnings = (
                QualityWarning(
                    code="W_DUPLICATE_KEY",
                    message=f"{dropped} duplicate source rows dropped (ETF stored under stock+fund)",
                    count=dropped,
                    severity="warning",
                ),
            )
        return _batch(ADAPTER_NAME, revision, request, view, tuple(records), warnings=warnings)

    def _read_intraday_bars(self, request: DataRequest, view: str, freq: str) -> DataBatch:
        """日内频率（如 30m）从 per-day ``{MMDD}/kline.parquet`` 读取（统一 kline 存储，timeframe 列）。"""
        wanted_asset = {"market.bar": None, "index.bar": "index", "fund.bar": "fund"}[view]
        step = _intraday_delta(freq)
        records: list[Bar] = []
        used_files: list[Path] = []
        for day in date_range_days(request.start, request.end):
            path = self._root / str(day.year) / day.strftime("%m%d") / "kline.parquet"
            if not path.is_file():
                continue
            used_files.append(path)
            try:
                frame = pd.read_parquet(path, filters=[("timeframe", "==", freq)])
            except Exception:
                frame = pd.read_parquet(path)
                if "timeframe" in frame.columns:
                    frame = frame[frame["timeframe"] == freq]
            if frame.empty:
                continue
            if request.instruments is not None:
                frame = frame[frame["ts_code"].isin(request.instruments)]
            if wanted_asset is not None:
                frame = frame[frame["data_type"].map(ASSET_TYPE_MAP) == wanted_asset]
            for row in frame.to_dict("records"):
                ts = pd.to_datetime(row.get("trade_time"), errors="coerce")
                if ts is None or ts is pd.NaT:
                    continue
                interval_end = ts.to_pydatetime().replace(tzinfo=CST)
                prices = [row.get(c) for c in ("open", "high", "low", "close")]
                if any(v is None or v != v for v in prices):
                    continue
                raw_volume = row.get("vol")
                raw_turnover = row.get("amount")
                records.append(Bar(
                    schema_version=SCHEMA_VERSION,
                    event_id=None,
                    instrument_id=str(row["ts_code"]),
                    asset_type=ASSET_TYPE_MAP.get(str(row.get("data_type")), "equity"),
                    effective_time=interval_end,
                    event_time=interval_end,
                    available_at=interval_end,
                    trading_date=day,
                    source=SOURCE,
                    quality="valid",
                    metadata={},
                    frequency=freq,
                    interval_start=interval_end - step,
                    interval_end=interval_end,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(raw_volume) * _BAR_CONVERSIONS["volume"] if raw_volume == raw_volume else 0.0,
                    turnover=float(raw_turnover) * _BAR_CONVERSIONS["turnover"] if raw_turnover == raw_turnover else 0.0,
                    is_complete=True,
                    price_basis="raw",
                ))
        records.sort(key=lambda bar: (bar.instrument_id, bar.interval_end))
        revision = source_revision(used_files) if used_files else "wp-empty"
        return _batch(ADAPTER_NAME, revision, request, view, tuple(records))

    # -- 每日估值指标 ---------------------------------------------------------


class WorkspaceTableAdapter:
    """提供 PIT 表数据集（每日估值指标、master 数据、成分归属）。"""

    DATASETS = ("market.daily_metric", "instrument.master", "industry.membership")

    def __init__(self, data_root: str | Path, market: str = MARKET) -> None:
        self._root = Path(data_root)
        self._market = market
        self._open_days_cache: list[date] | None = None
        self.descriptor = AdapterDescriptor(
            name=TABLE_ADAPTER_NAME,
            datasets={
                "market.daily_metric": _capability("market.daily_metric", frequencies=()),
                "instrument.master": _capability("instrument.master", frequencies=()),
                "industry.membership": _capability("industry.membership", frequencies=()),
            },
            historical_modes=("historical",),
            realtime_modes=(),
            supports_point_in_time=True,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset({"equity"}),
            schema_versions=(SCHEMA_VERSION,),
        )

    def read(self, request: DataRequest) -> DataBatch:
        if request.dataset == "market.daily_metric":
            return self._read_daily_metric(request)
        if request.dataset == "instrument.master":
            return self._read_instruments(request)
        if request.dataset == "industry.membership":
            return self._read_industry(request)
        raise ValueError(f"workspace-table adapter does not serve {request.dataset!r}")

    def iter(self, request: DataRequest, chunk_size: int = 10_000):
        yield self.read(request)

    def _read_daily_metric(self, request: DataRequest) -> DataBatch:
        files = resolve_year_dbs(self._root, request.start, request.end)
        revision = source_revision(files) if files else "wp-empty"
        if not files:
            return _batch(TABLE_ADAPTER_NAME, revision, request, "market.daily_metric", ())
        records: list[dict[str, Any]] = []
        start_s = to_yyyymmdd(request.start) if request.start is not None else None
        end_s = to_yyyymmdd(request.end) if request.end is not None else None
        for db_path in files:
            frame = read_table(db_path, "daily_basic").rename(
                columns={"ts_code": "instrument_id", "trade_date": "trading_date"}
            )
            if request.instruments is not None:
                frame = frame[frame["instrument_id"].isin(request.instruments)]
            if start_s is not None:
                frame = frame[frame["trading_date"] >= start_s]
            if end_s is not None:
                frame = frame[frame["trading_date"] <= end_s]
            for row in frame.to_dict("records"):
                trading_date = parse_yyyymmdd(row.get("trading_date"))
                if trading_date is None:
                    continue
                available_at = announcement_close(trading_date)
                if request.as_of is not None and available_at > request.as_of:
                    continue
                record: dict[str, Any] = {
                    "instrument_id": str(row["instrument_id"]),
                    "trading_date": trading_date,
                    "available_at": available_at,
                }
                for column, value in row.items():
                    if column in ("instrument_id", "trading_date", "limit"):
                        continue
                    if value is None or value != value:
                        record[column] = None
                        continue
                    if column in _METRIC_CONVERSIONS:
                        record[column] = float(value) * _METRIC_CONVERSIONS[column]
                    elif column in ("is_st", "is_suspended"):
                        record[column] = bool(int(value))
                    else:
                        record[column] = value
                records.append(_project(record, request.fields))
        records.sort(key=lambda r: (r["instrument_id"], r["trading_date"]))
        return _batch(TABLE_ADAPTER_NAME, revision, request, "market.daily_metric", tuple(records))

    # -- 标的 master ---------------------------------------------------------

    def _read_instruments(self, request: DataRequest) -> DataBatch:
        db_path = self._root / REFERENCE_DB
        if not db_path.is_file():
            raise FileNotFoundError(f"instrument master not found: {db_path}")
        frame = read_table(db_path, "stock_basic").rename(
            columns={"ts_code": "instrument_id", "list_date": "listed_date", "delist_date": "delisted_date"}
        )
        if request.instruments is not None:
            frame = frame[frame["instrument_id"].isin(request.instruments)]
        records: list[dict[str, Any]] = []
        for row in frame.to_dict("records"):
            listed = parse_yyyymmdd(row.get("listed_date"))
            delisted = parse_yyyymmdd(row.get("delisted_date"))
            available_at = announcement_close(listed) if listed else datetime(1990, 1, 1, 15, 0, tzinfo=CST)
            if request.as_of is not None and available_at > request.as_of:
                continue
            records.append(
                _project(
                    {
                        "instrument_id": str(row["instrument_id"]),
                        "symbol": str(row.get("symbol") or ""),
                        "name": str(row.get("name") or ""),
                        "asset_type": "equity",
                        "exchange": str(row.get("exchange") or ""),
                        "status": STATUS_MAP.get(str(row.get("list_status")), str(row.get("list_status"))),
                        "listed_date": listed,
                        "delisted_date": delisted,
                        "valid_from": listed,
                        "valid_to": delisted,
                        "available_at": available_at,
                    },
                    request.fields,
                )
            )
        records.sort(key=lambda r: (r["instrument_id"], r["valid_from"] or date.min))
        return _batch(TABLE_ADAPTER_NAME, source_revision([db_path]), request, "instrument.master", tuple(records))

    # -- 行业成分 -------------------------------------------------------------

    def _read_industry(self, request: DataRequest) -> DataBatch:
        db_path = self._root / REFERENCE_DB
        if not db_path.is_file():
            raise FileNotFoundError(f"industry membership not found: {db_path}")
        frame = read_table(db_path, "sw_industry").rename(
            columns={"index_code": "industry_id", "con_code": "instrument_id", "in_date": "valid_from", "out_date": "valid_to"}
        )
        if request.instruments is not None:
            frame = frame[frame["instrument_id"].isin(request.instruments)]
        if "industry_id" in request.filters:
            frame = frame[frame["industry_id"] == request.filters["industry_id"]]
        records: list[dict[str, Any]] = []
        for row in frame.to_dict("records"):
            valid_from = parse_yyyymmdd(row.get("valid_from"))
            if valid_from is None:
                continue
            available_at = announcement_close(valid_from)
            if request.as_of is not None and available_at > request.as_of:
                continue
            valid_to = parse_yyyymmdd(row.get("valid_to"))
            records.append(
                _project(
                    {
                        "industry_id": str(row["industry_id"]),
                        "instrument_id": str(row["instrument_id"]),
                        "level": 1,
                        "valid_from": valid_from,
                        "valid_to": valid_to,
                        "available_at": available_at,
                    },
                    request.fields,
                )
            )
        records.sort(key=lambda r: (r["industry_id"], r["instrument_id"], r["valid_from"]))
        return _batch(TABLE_ADAPTER_NAME, source_revision([db_path]), request, "industry.membership", tuple(records))


def _project(record: dict[str, Any], fields: tuple[str, ...] | None) -> dict[str, Any]:
    if not fields:
        return record
    return {key: record.get(key) for key in fields if key in record}
