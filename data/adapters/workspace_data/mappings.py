"""Migration mappings from legacy Tushare-layout data files to release datasets.

Every first-release dataset declared by ``tools.data.datasets.default_catalog``
has exactly one :class:`MigrationMapping` entry here.  The mapping is the
machine-auditable contract between the physical ``E:/ProgramData`` layout and
the standard catalog: column aliases, unit conversions, time semantics and the
conservative point-in-time rule all live in this module so that conversion
mistakes are visible and fixable in one place.

Physical layout (consolidated 2026-09-06):

- ``{root}/reference.duckdb``   one table per non-date-scoped master/reference
  table (``trade_date``, ``stock_basic``, ``fina_indicator``, ...);
- ``{root}/{year}/year.duckdb`` one table per date-scoped yearly table
  (``kline`` incl. multi-day timeframes, ``daily_basic``, ``margin``, ...);
- ``{root}/{year}/{MMDD}/stock|tradable|index.parquet``  per-day tick volumes
  (write-once, vendor-native);
- ``{root}/{year}/{MMDD}/kline.parquet``  that day's vendor daily-bar rows —
  the raw layer the yearly ``kline`` table is packed from.

A template's table is the ``#``-suffixed name (``{year}/year.duckdb#kline``).
``physical_sources`` is where readers find the data; ``write_targets`` (when
set) is where the downloader lands raw rows before packing.

Unit rules (validated against real data on 2026-09-06):

- kline ``vol`` is in lots (手) -> ``volume`` in shares (x100);
- kline ``amount`` is in thousand yuan (千元) -> ``turnover`` in yuan (x1000);
- daily_basic ``total_mv``/``circ_mv`` are in ten-thousand yuan -> yuan (x10000);
- daily_basic ``total_share``/``float_share``/``free_share`` are in ten-thousand
  shares -> shares (x10000);
- per-day tick ``vol`` is an increment in lots for equities (x100 -> shares,
  cross-checked: sum(vol) == kline vol, price implied 9.29 CNY vs close 9.21);
  index ticks keep the source-native unit because index quantity has no
  share-like base (documented, not converted);
- per-day tick ``amount`` is already yuan (no conversion);
- ``total_vol`` is the cumulative of ``vol`` per instrument per day
  (verified 4595/4595 rows), so ``vol`` is the event increment.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

DAY_CLOSE_PIT = "available_at = announcement date session close (15:00 Asia/Shanghai); date-level precision, conservative rule"
BAR_PIT = "market data: visible by event time; no announcement delay"
TRADE_PIT = "available_at == event_time (feed time); ReplayClock as_of injects visibility"

LEGACY_BAR = ("{year}/year.duckdb#kline",)
LEGACY_DAILY_BASIC = ("{year}/year.duckdb#daily_basic",)
LEGACY_TICK_DAY = ("{year}/{MMDD}/stock.parquet", "{year}/{MMDD}/tradable.parquet")
LEGACY_INDEX_TICK_DAY = ("{year}/{MMDD}/index.parquet",)
LEGACY_BAR_DAY_WRITE = ("{year}/{MMDD}/kline.parquet",)


@dataclass(frozen=True)
class MigrationMapping:
    """Auditable mapping from a legacy scope to one release dataset."""

    dataset: str
    status: str
    legacy_scope: tuple[str, ...]
    physical_sources: tuple[str, ...]
    field_aliases: Mapping[str, str] = field(default_factory=dict)
    unit_conversions: Mapping[str, str] = field(default_factory=dict)
    time_conversion: str = ""
    pit_rule: str = ""
    missing_semantics: str = ""
    source_revision_rule: str = "md5 over (path, size, mtime) of the source data files"
    implemented: bool = False
    write_targets: tuple[str, ...] = ()


_INTERNAL_SAMPLE = MigrationMapping(
    dataset="internal/sample",
    status="internal",
    legacy_scope=("internal/sample",),
    physical_sources=(),
    missing_semantics="strategy-internal control channel; rejected by the external gateway",
)


def _mapping(
    dataset: str,
    status: str,
    legacy_scope: tuple[str, ...],
    physical_sources: tuple[str, ...],
    *,
    aliases: Mapping[str, str] | None = None,
    conversions: Mapping[str, str] | None = None,
    time_conversion: str = "",
    pit_rule: str = "",
    missing_semantics: str = "",
    source_revision_rule: str = "md5 over (path, size, mtime) of the source data files",
    implemented: bool = False,
    write_targets: tuple[str, ...] = (),
) -> MigrationMapping:
    return MigrationMapping(
        dataset=dataset,
        status=status,
        legacy_scope=legacy_scope,
        physical_sources=physical_sources,
        field_aliases=dict(aliases or {}),
        unit_conversions=dict(conversions or {}),
        time_conversion=time_conversion,
        pit_rule=pit_rule,
        missing_semantics=missing_semantics,
        source_revision_rule=source_revision_rule,
        implemented=implemented,
        write_targets=write_targets,
    )


_YEAR_TABLE = "one parquet file per year, resolved as {root}/{year}/{file}"
_GLOBAL_TABLE = "one parquet file at {root}/{file}"

MIGRATION_MAPPINGS: tuple[MigrationMapping, ...] = (
    # -- phase 1: core market chain (implemented) ----------------------------
    _mapping(
        "calendar.session",
        "available",
        ("calendar/trade_cal",),
        ("{root}/reference.duckdb#trade_date",),
        aliases={"cal_date": "trading_date", "is_open": "is_open"},
        time_conversion="cal_date YYYYMMDD -> trading_date; single regular phase 09:30-15:00 Asia/Shanghai",
        pit_rule="calendar is not PIT-gated",
        implemented=True,
    ),
    _mapping(
        "instrument.master",
        "available",
        ("stock/basic",),
        ("{root}/reference.duckdb#stock_basic",),
        aliases={
            "ts_code": "instrument_id",
            "symbol": "symbol",
            "name": "name",
            "exchange": "exchange",
            "list_status": "status",
            "list_date": "listed_date",
            "delist_date": "delisted_date",
        },
        conversions={"list_status": "L->listed, D->delisted, P->paused"},
        time_conversion="list_date/delist_date YYYYMMDD -> dates; valid_from=listed_date, valid_to=delisted_date or open",
        pit_rule=DAY_CLOSE_PIT,
        missing_semantics="delist_date empty for listed instruments -> open validity end",
        implemented=True,
    ),
    _mapping(
        "industry.membership",
        "available",
        ("sw/industry",),
        ("{root}/reference.duckdb#sw_industry",),
        aliases={"index_code": "industry_id", "con_code": "instrument_id", "in_date": "valid_from", "out_date": "valid_to"},
        conversions={"is_new": "source-native flag, kept in metadata"},
        time_conversion="in_date/out_date YYYYMMDD -> validity window; source has no level column -> level='L1'",
        pit_rule=DAY_CLOSE_PIT,
        missing_semantics="out_date empty -> open membership end",
        implemented=True,
    ),
    _mapping(
        "market.bar",
        "available",
        ("trade_data/daily",),
        LEGACY_BAR,
        aliases={
            "ts_code": "instrument_id",
            "trade_date": "trading_date",
            "vol": "volume",
            "amount": "turnover",
            "adj_factor": "adj_factor",
        },
        conversions={"vol": "x100 lots -> shares", "amount": "x1000 thousand-yuan -> yuan"},
        time_conversion="trade_date YYYYMMDD -> trading_date; interval 09:30-15:00 Asia/Shanghai; event_time=interval_end",
        pit_rule=BAR_PIT,
        missing_semantics="pct_chg/change keep sign semantics; NaN change on first bar",
        source_revision_rule="md5 over (path, size, mtime) of the yearly duckdb files used",
        implemented=True,
        write_targets=LEGACY_BAR_DAY_WRITE,
    ),
    _mapping(
        "market.daily_metric",
        "available",
        ("trade_data/daily 的估值与状态字段",),
        LEGACY_DAILY_BASIC,
        aliases={
            "ts_code": "instrument_id",
            "trade_date": "trading_date",
        },
        conversions={
            "total_mv": "x10000 ten-thousand-yuan -> yuan",
            "circ_mv": "x10000 ten-thousand-yuan -> yuan",
            "total_share": "x10000 ten-thousand-shares -> shares",
            "float_share": "x10000 ten-thousand-shares -> shares",
            "free_share": "x10000 ten-thousand-shares -> shares",
        },
        time_conversion="trade_date YYYYMMDD -> trading_date; available_at = trading_date session close (after-close publication)",
        pit_rule=DAY_CLOSE_PIT,
        missing_semantics="is_st/is_suspended NaN -> not asserted (None), not treated as False",
        implemented=True,
    ),
    _mapping(
        "market.trade",
        "available",
        ("trade_data/tick",),
        LEGACY_TICK_DAY + LEGACY_INDEX_TICK_DAY,
        aliases={"code": "instrument_id", "pr": "price", "vol": "size", "amount": "turnover", "bs": "side"},
        conversions={
            "vol": "x100 lots -> shares for equity; index ticks keep source-native unit",
            "amount": "already yuan, no conversion",
        },
        time_conversion="directory YYYY/MMDD -> trading_date; row time HH:MM:SS -> event_time (Asia/Shanghai)",
        pit_rule=TRADE_PIT,
        missing_semantics="pr==0 rows are no-trade snapshots -> no TradeTick emitted",
        source_revision_rule="md5 over (path, size, mtime) of the per-day files used",
        implemented=True,
    ),
    _mapping(
        "market.quote",
        "available",
        ("trade_data/tick",),
        LEGACY_TICK_DAY,
        aliases={"code": "instrument_id", "pr": "last_price", "vol": "last_size"},
        conversions={
            "b1v..b5v/s1v..s5v": "x100 lots -> shares for equity; index quotes not provided by source",
        },
        time_conversion="same as market.trade",
        pit_rule=TRADE_PIT,
        missing_semantics="zero prices/levels are dropped from the level tuples; empty tuple means no quote, never zero",
        implemented=True,
    ),
    _mapping(
        "index.bar",
        "available",
        ("index/daily",),
        LEGACY_BAR,
        aliases={"ts_code": "instrument_id", "trade_date": "trading_date", "vol": "volume", "amount": "turnover"},
        conversions={"vol": "index source-native unit (kept), amount x1000 thousand-yuan -> yuan"},
        time_conversion="kline rows with data_type=index -> index.bar view of market.bar",
        pit_rule=BAR_PIT,
        implemented=True,
        write_targets=LEGACY_BAR_DAY_WRITE,
    ),
    _mapping(
        "fund.bar",
        "available",
        ("fund/daily",),
        LEGACY_BAR,
        aliases={"ts_code": "instrument_id", "trade_date": "trading_date", "vol": "volume", "amount": "turnover"},
        conversions={"vol": "x100 lots -> shares", "amount": "x1000 thousand-yuan -> yuan"},
        time_conversion="kline rows with data_type=fund -> fund.bar view of market.bar",
        pit_rule=BAR_PIT,
        implemented=True,
        write_targets=LEGACY_BAR_DAY_WRITE,
    ),
    _mapping(
        "market.daily_snapshot",
        "available",
        ("kline.parquet + daily_basic.parquet 复合查询",),
        LEGACY_BAR + LEGACY_DAILY_BASIC,
        time_conversion="join of market.bar and market.daily_metric on (instrument_id, trading_date)",
        pit_rule="max(bar available_at, metric available_at)",
        implemented=True,
    ),
    # -- phase 2: mapped, adapters pending ------------------------------------
    _mapping(
        "market.money_flow",
        "available",
        ("trade_data/moneyflow",),
        ("{year}/year.duckdb#moneyflow",),
        aliases={"ts_code": "instrument_id", "trade_date": "trading_date"},
        conversions={"*_amount": "x1000 thousand-yuan -> yuan (Tushare moneyflow amounts)", "*_vol": "x100 lots -> shares"},
        time_conversion="trade_date YYYYMMDD -> trading_date",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "market.margin",
        "available",
        ("idx=daily_margin",),
        ("{year}/year.duckdb#margin",),
        aliases={"trade_date": "trading_date"},
        conversions={"rzmre/rzye/rqye/rzrqye": "yuan, no conversion"},
        time_conversion="rows with level=summary -> market.margin",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "market.margin_detail",
        "available",
        ("trade_data/margin_detail",),
        ("{year}/year.duckdb#margin",),
        aliases={"ts_code": "instrument_id", "trade_date": "trading_date"},
        time_conversion="rows with level=detail -> market.margin_detail",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "market.breadth",
        "derived",
        ("trade_data/breadth",),
        ("{year}/year.duckdb#market_breadth",),
        aliases={"trade_date": "trading_date"},
        conversions={"up_amount/down_amount/total_amount": "yuan, no conversion"},
        time_conversion="trade_date YYYYMMDD -> trading_date",
        pit_rule=DAY_CLOSE_PIT,
        missing_semantics="second-level aggregation derived from market data, not a vendor fact",
    ),
    _mapping(
        "market.northbound",
        "available",
        ("trade_data/northbound",),
        ("{year}/year.duckdb#northbound_netbuy",),
        aliases={"ts_code": "instrument_id", "trade_date": "trading_date"},
        time_conversion="trade_date YYYYMMDD -> trading_date; source present for 2023 only",
        pit_rule=DAY_CLOSE_PIT,
        missing_semantics="no northbound_netbuy file for years other than 2023",
    ),
    _mapping(
        "market.northbound_summary",
        "available",
        ("trade_data/moneyflow_hsgt",),
        ("{year}/year.duckdb#moneyflow_hsgt",),
        aliases={"trade_date": "trading_date"},
        time_conversion="trade_date YYYYMMDD -> trading_date",
        pit_rule="publication time and trading date are separate in source",
    ),
    _mapping(
        "market.shibor",
        "available",
        ("trade_data/shibor",),
        ("{year}/year.duckdb#shibor",),
        aliases={"date": "observation_date"},
        conversions={"on/1w/2w/1m/3m/6m/9m/1y": "percentage_points, no conversion; wide table -> (observation_date, term, rate) long rows"},
        time_conversion="date YYYYMMDD -> observation_date",
        pit_rule="observation and publication time are separate",
    ),
    _mapping(
        "market.yield_curve",
        "available",
        ("trade_data/yc_cb",),
        ("{year}/year.duckdb#yc_cb",),
        aliases={"trade_date": "observation_date", "ts_code": "curve_id"},
        conversions={"yield": "percentage_points, no conversion"},
        time_conversion="curve_name/curve_type/curve_term are dimensions",
        pit_rule="observation and publication time are separate",
    ),
    _mapping(
        "fund.portfolio",
        "available",
        ("fund/portfolio",),
        ("{root}/reference.duckdb#fund_portfolio",),
        aliases={
            "fund_code": "fund_id",
            "ts_code": "instrument_id",
            "end_date": "report_date",
            "mkv": "market_value",
            "amount": "quantity",
            "ann_date": "available_at",
        },
        conversions={"mkv": "x10000 ten-thousand-yuan -> yuan", "amount": "x10000 ten-thousand-shares -> shares"},
        time_conversion="end_date YYYYMMDD -> report_date; ann_date -> available_at",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "fundamental.indicator",
        "available",
        ("fina/indicator",),
        ("{root}/reference.duckdb#fina_indicator",),
        aliases={"ts_code": "instrument_id", "end_date": "report_date", "ann_date": "available_at"},
        time_conversion="end_date YYYYMMDD -> report_date (effective_time); ann_date YYYYMMDD -> available_at",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "fundamental.income",
        "available",
        ("fina/income_report",),
        ("{root}/reference.duckdb#income_report",),
        aliases={"ts_code": "instrument_id", "end_date": "report_date", "ann_date": "available_at", "f_ann_date": "available_at"},
        conversions={"*_income/revenue/total_*": "yuan, no conversion for this source"},
        time_conversion="end_date -> report_date; f_ann_date (fallback ann_date) -> available_at",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "fundamental.balance",
        "available",
        ("fina/balance_report",),
        ("{root}/reference.duckdb#balance_report",),
        aliases={"ts_code": "instrument_id", "end_date": "report_date", "ann_date": "available_at", "f_ann_date": "available_at"},
        time_conversion="end_date -> report_date; f_ann_date (fallback ann_date) -> available_at",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "fundamental.cashflow",
        "available",
        ("fina/cashflow_report",),
        ("{root}/reference.duckdb#cashflow_report",),
        aliases={"ts_code": "instrument_id", "end_date": "report_date", "ann_date": "available_at", "f_ann_date": "available_at"},
        time_conversion="end_date -> report_date; f_ann_date (fallback ann_date) -> available_at",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "event.forecast",
        "available",
        ("event/forecast",),
        ("{root}/reference.duckdb#forecast",),
        aliases={"ts_code": "instrument_id", "ann_date": "available_at", "end_date": "report_date"},
        time_conversion="ann_date -> available_at; end_date -> report_date",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "event.holder_trade",
        "available",
        ("event/holdertrade",),
        ("{root}/reference.duckdb#holdertrade",),
        aliases={"ts_code": "instrument_id", "ann_date": "available_at", "holder_name": "holder_name"},
        time_conversion="ann_date -> available_at; begin_date/close_date -> validity window",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "event.top_holder",
        "available",
        ("event/top10_holder",),
        ("{root}/reference.duckdb#top10_holder",),
        aliases={"ts_code": "instrument_id", "ann_date": "available_at", "end_date": "report_date", "holder_num": "holder_number"},
        time_conversion="ann_date -> available_at; end_date -> report_date",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "event.holder_number",
        "available",
        ("event/stk_holdernumber",),
        ("{year}/year.duckdb#stk_holdernumber",),
        aliases={"ts_code": "instrument_id", "ann_date": "available_at", "end_date": "report_date", "holder_num": "holder_number"},
        time_conversion="ann_date -> available_at; end_date -> report_date",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "event.block_trade",
        "available",
        ("event/block_trade",),
        ("{year}/year.duckdb#block_trade",),
        aliases={"ts_code": "instrument_id", "trade_date": "event_date"},
        conversions={"vol": "x100 lots -> shares", "amount": "x1000 thousand-yuan -> yuan"},
        time_conversion="trade_date -> event_date/effective_time",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "event.pledge",
        "available",
        ("event/pledge_stat",),
        ("{year}/year.duckdb#pledge_stat",),
        aliases={"ts_code": "instrument_id", "end_date": "report_date", "trade_date": "available_at"},
        time_conversion="end_date -> report period; trade_date -> available_at",
        pit_rule=DAY_CLOSE_PIT,
    ),
    _mapping(
        "macro.indicator",
        "available",
        ("macro/indicator",),
        ("{root}/reference.duckdb#macro",),
        aliases={"month": "observation_month", "indicator": "indicator", "value": "value"},
        time_conversion="month -> observation_month",
        pit_rule="publication time not present in source -> contract_only-grade PIT, use conservatively",
    ),
    # -- contract-only / provisional: no physical source ----------------------
    _mapping(
        "fundamental.consensus",
        "available",
        ("fina/consensus",),
        ("fina/consensus",),
        missing_semantics="no physical file on the workspace volume; requests fail with UnsupportedDatasetError",
    ),
    _mapping(
        "event.equity_incentive",
        "contract_only",
        ("event/equity_incentive",),
        (),
        missing_semantics="no unified physical table on the workspace volume",
    ),
    _mapping(
        "event.private_placement",
        "contract_only",
        ("event/private_placement",),
        (),
        missing_semantics="explicitly marked as pending integration in the workspace",
    ),
    _mapping(
        "analyst.rating",
        "contract_only",
        ("analyst/rating",),
        (),
        missing_semantics="strategy-side assumption only; no source",
    ),
    _mapping(
        "index.member",
        "available",
        ("index/member",),
        ("{year}/year.duckdb#index_member",),
        aliases={"index_id": "index_id", "ts_code": "instrument_id", "trade_date": "effective_date"},
        time_conversion="per-day constituent files -> (index_id, instrument_id, effective_date) rows",
        pit_rule=DAY_CLOSE_PIT,
        missing_semantics="per-day constituent files not present on the current USB volume; phase-2 adapter",
    ),
    _mapping(
        "corporate.action",
        "available",
        ("复权因子",),
        ("{year}/year.duckdb#kline adj_factor",),
        time_conversion="adj_factor changes at ex-dividend dates; effective_time = ex-date",
        pit_rule="available_at = ex-date session close (conservative until a dedicated source exists)",
        missing_semantics="phase 2: no dedicated adapter yet; seeded from kline adj_factor later",
    ),
    _mapping(
        "derived.technical_indicator",
        "derived",
        ("idx=indicator",),
        ("tools.indicators.IndicatorProvider",),
        time_conversion="computed from market.bar; input as_of propagates to results",
        pit_rule="derived at request time; provenance carries input fingerprint and calculation version",
    ),
)

MAPPINGS_BY_DATASET: dict[str, MigrationMapping] = {m.dataset: m for m in MIGRATION_MAPPINGS}

__all__ = [
    "DAY_CLOSE_PIT",
    "BAR_PIT",
    "TRADE_PIT",
    "MigrationMapping",
    "MIGRATION_MAPPINGS",
    "MAPPINGS_BY_DATASET",
    "_INTERNAL_SAMPLE",
]
