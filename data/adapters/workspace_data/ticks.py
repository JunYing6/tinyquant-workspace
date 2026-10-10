"""每日 tick 适配器：``market.trade`` 与 ``market.quote``。

物理数据源：``{root}/{year}/{MMDD}/stock.parquet``（股票，五档盘口）以及
``{root}/{year}/{MMDD}/index.parquet``（指数成交）。一行可能同时携带一笔
成交和一个盘口状态：``pr > 0`` 的行生成 ``TradeTick``，含任意有效盘口档位的
行生成 ``QuoteTick``（当两个事实都被证明时，设计允许从同一数据行生成两者；
``pr == 0`` 的行是仅报价快照，绝不会伪造成交）。

序列号为合成值（每个标的每天的逐行顺序），并以 ``SEQUENCE_SYNTHETIC``
质量告警上报；因此严格的跨标的重放不能依赖全局顺序（数据源仅声明按标的
排序）。
"""

from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from tools.data import (
    AdapterDescriptor,
    DataBatch,
    DataProvenance,
    DataRequest,
    DatasetCapability,
    PriceLevel,
    QualityReport,
    QualityWarning,
    TradeTick,
    QuoteTick,
    default_catalog,
)

from data.adapters.workspace_data.reader import CST, date_range_days, source_revision

ADAPTER_NAME = "workspace-tick"
SCHEMA_VERSION = "1"
SOURCE = "workspace-tick"

_EQUITY_SIZE_FACTOR = 100.0  # 手 -> 股（股票）
_INDEX_SIZE_FACTOR = 1.0  # 指数量保留数据源原生单位

_SIDE_MAP = {"B": "BUY", "S": "SELL"}
_LEVEL_COLUMNS = tuple((f"b{i}p", f"b{i}v") for i in range(1, 6))
_ASK_LEVEL_COLUMNS = tuple((f"s{i}p", f"s{i}v") for i in range(1, 6))


def _capability(dataset: str) -> DatasetCapability:
    definition = default_catalog().get(dataset)
    return DatasetCapability(
        dataset=dataset,
        modes=("historical",),
        asset_types=frozenset({"equity", "index"}),
        frequencies=(),
        fields=tuple(definition.fields),
        point_in_time=definition.point_in_time,
    )


class WorkspaceTickAdapter:
    """从每日 tick 文件提供 ``market.trade`` / ``market.quote``。"""

    DATASETS = ("market.trade", "market.quote")

    def __init__(self, data_root: str | Path, market: str = "CN") -> None:
        self._root = Path(data_root)
        self.descriptor = AdapterDescriptor(
            name=ADAPTER_NAME,
            datasets={
                "market.trade": _capability("market.trade"),
                "market.quote": _capability("market.quote"),
            },
            historical_modes=("historical",),
            realtime_modes=(),
            supports_point_in_time=True,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset({"equity", "index"}),
            schema_versions=(SCHEMA_VERSION,),
            ordering_guarantee="per_instrument",
        )

    def read(self, request: DataRequest) -> DataBatch:
        if request.dataset == "market.trade":
            return self._read(request, want_trades=True, want_quotes=False)
        if request.dataset == "market.quote":
            return self._read(request, want_trades=False, want_quotes=True)
        raise ValueError(f"workspace-tick adapter does not serve {request.dataset!r}")

    def iter(self, request: DataRequest, chunk_size: int = 10_000):
        """DuckDB 流式 tick 迭代：``WHERE code IN(...) AND time>=.. AND time<.. ORDER BY time, code``。

        结果本身全局时间有序，直接 ``fetchmany`` 分批吐；一次取一批、逐行转事件，不整表物化。
        """
        if request.start is None or request.end is None:
            raise ValueError("market.trade/quote requests require start and end (session bounds)")
        days = date_range_days(request.start, request.end)
        asset_filter = request.asset_type if request.asset_type in ("equity", "index") else None
        asset_types = (asset_filter,) if asset_filter else ("equity", "index")
        instruments = tuple(str(code) for code in (request.instruments or ()))
        want_trades = request.dataset == "market.trade"
        want_quotes = request.dataset == "market.quote"
        for asset_type in asset_types:
            for day in days:
                path = self._day_file(day, asset_type)
                if not path.is_file():
                    continue
                factor = _EQUITY_SIZE_FACTOR if asset_type == "equity" else _INDEX_SIZE_FACTOR
                con, result, names = self._open_query(path, day, request, instruments)
                try:
                    counters: dict[str, int] = {}
                    buffer: list[Any] = []
                    while True:
                        rows = result.fetchmany(chunk_size)
                        if not rows:
                            break
                        for row in rows:
                            for event in self._rows_to_events(dict(zip(names, row)), day, asset_type, factor, counters, want_trades, want_quotes, request.as_of):
                                buffer.append(event)
                        if len(buffer) >= chunk_size:
                            yield self._batch(request, buffer, [])
                            buffer = []
                    if buffer:
                        yield self._batch(request, buffer, [])
                finally:
                    con.close()

    def _open_query(self, path: Path, day: date, request: DataRequest, instruments: tuple[str, ...]):
        """DuckDB 查询：按 code + 时间过滤，`ORDER BY time, code` 输出全局时间有序。"""
        import duckdb

        first_day = day == (request.start.date() if isinstance(request.start, datetime) else request.start)
        last_day = day == (request.end.date() if isinstance(request.end, datetime) else request.end)
        start_t = request.start.time() if isinstance(request.start, datetime) and first_day else time(0, 0, 0)
        end_t = request.end.time() if isinstance(request.end, datetime) and last_day else time(23, 59, 59)
        if instruments:
            marks = ",".join("?" * len(instruments))
            where = f"code IN ({marks}) AND time >= ? AND time < ?"
            params: list[Any] = [*instruments, start_t.strftime("%H:%M:%S"), end_t.strftime("%H:%M:%S")]
        else:
            where = "time >= ? AND time < ?"
            params = [start_t.strftime("%H:%M:%S"), end_t.strftime("%H:%M:%S")]
        path_literal = str(path).replace("'", "''")
        sql = f"SELECT * FROM read_parquet('{path_literal}') WHERE {where} ORDER BY time, code"
        con = duckdb.connect()
        result = con.execute(sql, params)
        names = tuple(d[0] for d in result.description)
        return con, result, names

    def _read(self, request: DataRequest, *, want_trades: bool, want_quotes: bool) -> DataBatch:
        if request.start is None or request.end is None:
            raise ValueError("market.trade/quote requests require start and end (session bounds)")
        days = date_range_days(request.start, request.end)
        asset_filter = request.asset_type if request.asset_type in ("equity", "index") else None
        asset_types = (asset_filter,) if asset_filter else ("equity", "index")
        instruments = tuple(str(code) for code in (request.instruments or ()))

        records: list[Any] = []
        used_files: list[Path] = []
        for asset_type in asset_types:
            for day in days:
                path = self._day_file(day, asset_type)
                if not path.is_file():
                    continue
                used_files.append(path)
                factor = _EQUITY_SIZE_FACTOR if asset_type == "equity" else _INDEX_SIZE_FACTOR
                con, result, names = self._open_query(path, day, request, instruments)
                try:
                    counters: dict[str, int] = {}
                    for row in result.fetchmany(20_000):
                        records.extend(self._rows_to_events(dict(zip(names, row)), day, asset_type, factor, counters, want_trades, want_quotes, request.as_of))
                finally:
                    con.close()
        records.sort(key=lambda r: (r.instrument_id, r.event_time, r.sequence))
        return self._batch(request, records, used_files)

    def _rows_to_events(
        self,
        row: dict[str, Any],
        day: date,
        asset_type: str,
        factor: float,
        counters: dict[str, int],
        want_trades: bool,
        want_quotes: bool,
        as_of: datetime | None,
    ) -> list[Any]:
        """把 DuckDB 一行转成 0~2 个事件（同行的成交 + 盘口）。"""
        code = str(row.get("code"))
        event_time = self._event_time(day, str(row.get("time")))
        if as_of is not None and event_time > as_of:
            return []
        sequence = counters.get(code, -1) + 1
        counters[code] = sequence
        price = float(row.get("pr") or 0.0)
        bid_levels, ask_levels = self._levels(row, asset_type)
        events: list[Any] = []
        if want_quotes and (bid_levels or ask_levels):
            events.append(QuoteTick(
                schema_version=SCHEMA_VERSION,
                event_id=None,
                instrument_id=code,
                asset_type=asset_type,
                effective_time=event_time,
                event_time=event_time,
                available_at=event_time,
                trading_date=day,
                source=SOURCE,
                quality="valid",
                metadata={},
                event_type="quote",
                bid_levels=tuple(bid_levels),
                ask_levels=tuple(ask_levels),
                last_price=price if price > 0 else None,
                last_size=float(row.get("vol") or 0.0) * factor if float(row.get("vol") or 0.0) > 0 else None,
                sequence=sequence,
            ))
        if want_trades and price > 0:
            events.append(TradeTick(
                schema_version=SCHEMA_VERSION,
                event_id=None,
                instrument_id=code,
                asset_type=asset_type,
                effective_time=event_time,
                event_time=event_time,
                available_at=event_time,
                trading_date=day,
                source=SOURCE,
                quality="valid",
                metadata={},
                event_type="trade",
                price=price,
                size=float(row.get("vol") or 0.0) * factor,
                turnover=float(row.get("amount") or 0.0),
                side=_SIDE_MAP.get(str(row.get("bs")), "UNKNOWN"),
                sequence=sequence,
            ))
        return events

    def _day_file(self, day: date, asset_type: str) -> Path:
        sub = day.strftime("%m%d")
        name = "stock.parquet" if asset_type == "equity" else "index.parquet"
        return self._root / str(day.year) / sub / name

    @staticmethod
    def _event_time(day: date, clock: str) -> datetime:
        parts = [int(piece) for piece in str(clock).split(":")]
        while len(parts) < 3:
            parts.append(0)
        return datetime(day.year, day.month, day.day, parts[0], parts[1], parts[2], tzinfo=CST)

    def _levels(self, row: dict[str, Any], asset_type: str) -> tuple[list[PriceLevel], list[PriceLevel]]:
        factor = _EQUITY_SIZE_FACTOR if asset_type == "equity" else _INDEX_SIZE_FACTOR
        bids: list[PriceLevel] = []
        asks: list[PriceLevel] = []
        for level, (price_col, size_col) in enumerate(_LEVEL_COLUMNS, start=1):
            price = float(row.get(price_col) or 0.0)
            if price > 0:
                bids.append(PriceLevel(price=price, size=float(row.get(size_col) or 0.0) * factor, level=level))
        for level, (price_col, size_col) in enumerate(_ASK_LEVEL_COLUMNS, start=1):
            price = float(row.get(price_col) or 0.0)
            if price > 0:
                asks.append(PriceLevel(price=price, size=float(row.get(size_col) or 0.0) * factor, level=level))
        return bids, asks

    def _batch(self, request: DataRequest, records: list[Any], used_files: list[Path]) -> DataBatch:
        warnings: tuple[QualityWarning, ...] = ()
        status = "ok"
        if records:
            warnings = (
                QualityWarning(
                    code="SEQUENCE_SYNTHETIC",
                    message="source has no sequence column; per-instrument row order used",
                    count=len(records),
                    severity="warning",
                ),
            )
            status = "warning"
        dataset = request.dataset
        revision = source_revision(used_files) if used_files else "wp-empty"
        return DataBatch(
            request_id=request.correlation_id or "",
            dataset=dataset,
            schema_version=SCHEMA_VERSION,
            correlation_id=request.correlation_id,
            records=tuple(records),
            complete=True,
            next_cursor=None,
            provenance=DataProvenance(
                adapter_name=ADAPTER_NAME,
                source_revision=revision,
                request_fingerprint=request.delivery_key or "workspace",
                read_at=datetime.now(tz=CST),
            ),
            quality=QualityReport(status=status, warnings=warnings, checked_count=len(records)),
        )
