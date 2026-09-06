"""Tushare (and legacy non-Tushare) scope registry and the download facade.

The registry maps every legacy scope declared by
``data.adapters.workspace_data.mappings`` to a :class:`ScopeSpec`:

    scope key -> fetcher(client, start, end, **options) -> raw frame(s)

Fetchers return vendor-native frames (no renaming, no unit conversion — the
rules live in the ``MigrationMapping``); ``TushareDownloader.download`` lands
them in the mapped physical layout via ``RawVolumeWriter``.  The client is
always injected by the caller — nothing here reads tokens or credentials::

    import tushare as ts
    client = ts.pro_api(token)          # caller's responsibility
    downloader = TushareDownloader(client=client, root="E:/ProgramData")
    downloader.download("trade_data/daily", "20240102", "20241231")

Scopes that need trading days resolve them from the injected
``trade_dates_provider`` or, by default, from the vendor ``trade_cal``
interface itself (see ``scopes.calendar``).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from data.downloader.base import RawVolumeWriter
from data.downloader.scopes import (
    balance,
    block_trade,
    calendar,
    cashflow,
    consensus,
    daily,
    fina,
    forecast,
    fund_daily,
    fund_portfolio,
    holdertrade,
    income,
    index,
    index_member,
    macro,
    margin,
    margin_detail,
    market_breadth,
    minute,
    moneyflow,
    moneyflow_hsgt,
    northbound,
    pledge_stat,
    shibor,
    stk_holdernumber,
    stock_basic,
    sw_industry,
    top10_holder,
    yc_cb,
)

from data.downloader import tick_import

# fetchers receive: (client, start, end, *, sleep, calendar, data_root, **options)
FetchFn = Callable[..., Any]
# custom writers receive: (writer, payload, *, start, end, **options) -> list[Path]
WriteFn = Callable[..., list]
DedupKeys = Sequence[str] | Mapping[str, Sequence[str]]


@dataclass(frozen=True)
class ScopeSpec:
    """Registry entry wiring one legacy scope to its fetch + write contract.

    Attributes:
        fetch: vendor fetcher returning a raw frame, a dict of
            ``{dataset_name: frame}`` (multi-dataset scopes), or ``None``
            (scopes whose work happens entirely in ``write``).
        date_column: column used to group rows into ``{year}``[/``{MMDD}``]
            files; ``None`` for global snapshot tables.
        dedup_keys: business key for merge-dedupe (``keep='last'``), either a
            column tuple or a per-dataset mapping for multi-dataset scopes.
            ``None`` deduplicates on the full row.
        write: optional custom writer overriding the mapping-driven write
            (used by layouts the ``MigrationMapping`` templates cannot
            express, e.g. per-day ``index_member`` files).
    """

    fetch: FetchFn
    date_column: str | None = "trade_date"
    dedup_keys: DedupKeys | None = None
    write: WriteFn | None = None
    description: str = ""


_SCOPE_FETCHERS: dict[str, ScopeSpec] = {
    # -- market ---------------------------------------------------------------
    "trade_data/daily": ScopeSpec(
        fetch=daily.fetch_daily,
        dedup_keys={
            "market.bar": ("ts_code", "trade_date", "data_type", "timeframe"),
            "market.daily_metric": ("ts_code", "trade_date"),
        },
        description="daily bars -> {year}/kline.parquet + daily metrics -> {year}/daily_basic.parquet",
    ),
    "trade_data/tick": ScopeSpec(
        fetch=tick_import.fetch_tick_import,
        date_column=None,
        write=tick_import.write_tick_import,
        description="tick CSV ingestion -> {year}/{MMDD}/{stock,tradable,index}.parquet",
    ),
    "trade_data/minute": ScopeSpec(
        fetch=minute.fetch_minute,
        write=minute.write_minute,
        description="intraday bars -> {year}/minute_{freq}.parquet (no mapping template)",
    ),
    "trade_data/moneyflow": ScopeSpec(
        fetch=moneyflow.fetch_moneyflow,
        dedup_keys=("trade_date", "ts_code"),
        description="per-stock moneyflow -> {year}/moneyflow.parquet",
    ),
    "idx=daily_margin": ScopeSpec(
        fetch=margin.fetch_margin,
        dedup_keys=("trade_date", "level"),
        description="margin summary rows -> {year}/margin.parquet (level=summary)",
    ),
    "trade_data/margin_detail": ScopeSpec(
        fetch=margin_detail.fetch_margin_detail,
        dedup_keys=("trade_date", "ts_code", "level"),
        description="per-stock margin detail -> {year}/margin.parquet (level=detail)",
    ),
    "trade_data/breadth": ScopeSpec(
        fetch=market_breadth.fetch_market_breadth,
        dedup_keys=("trade_date",),
        description="market breadth aggregated from the kline volume -> {year}/market_breadth.parquet",
    ),
    "trade_data/northbound": ScopeSpec(
        fetch=northbound.fetch_northbound,
        dedup_keys=("ts_code", "trade_date"),
        description="per-stock northbound net buy (akshare) -> {year}/northbound_netbuy.parquet",
    ),
    "trade_data/moneyflow_hsgt": ScopeSpec(
        fetch=moneyflow_hsgt.fetch_moneyflow_hsgt,
        dedup_keys=("trade_date",),
        description="HSGT north/south flow -> {year}/moneyflow_hsgt.parquet",
    ),
    "trade_data/shibor": ScopeSpec(
        fetch=shibor.fetch_shibor,
        date_column="date",
        dedup_keys=("date",),
        description="SHIBOR quotes -> {year}/shibor.parquet",
    ),
    "trade_data/yc_cb": ScopeSpec(
        fetch=yc_cb.fetch_yc_cb,
        dedup_keys=("ts_code", "curve_term", "trade_date"),
        description="bond yield curve -> {year}/yc_cb.parquet",
    ),
    "index/daily": ScopeSpec(
        fetch=index.fetch_index_daily,
        dedup_keys=("ts_code", "trade_date", "data_type", "timeframe"),
        description="index daily bars -> {year}/kline.parquet (data_type=index)",
    ),
    "fund/daily": ScopeSpec(
        fetch=fund_daily.fetch_fund_daily,
        dedup_keys=("ts_code", "trade_date", "data_type", "timeframe"),
        description="fund daily bars -> {year}/kline.parquet (data_type=fund)",
    ),
    "index/member": ScopeSpec(
        fetch=index_member.fetch_index_member,
        write=index_member.write_index_member,
        description="index constituent weights -> {year}/{MMDD}/index_member.parquet",
    ),
    # -- reference ------------------------------------------------------------
    "calendar/trade_cal": ScopeSpec(
        fetch=calendar.fetch_trade_cal,
        date_column=None,
        dedup_keys=("cal_date",),
        description="exchange calendar -> {root}/trade_date.parquet",
    ),
    "stock/basic": ScopeSpec(
        fetch=stock_basic.fetch_stock_basic,
        date_column=None,
        dedup_keys=("ts_code",),
        description="instrument master -> {root}/stock_basic.parquet",
    ),
    "sw/industry": ScopeSpec(
        fetch=sw_industry.fetch_sw_industry,
        date_column=None,
        dedup_keys=None,
        description="SW L1 membership -> {root}/sw_industry.parquet",
    ),
    # -- fundamentals -----------------------------------------------------------
    "fina/indicator": ScopeSpec(
        fetch=fina.fetch_fina_indicator,
        date_column=None,
        dedup_keys=("ts_code", "end_date", "ann_date"),
        description="financial indicators -> {root}/fina_indicator.parquet",
    ),
    "fina/income_report": ScopeSpec(
        fetch=income.fetch_income,
        date_column=None,
        dedup_keys=("ts_code", "end_date", "ann_date"),
        description="income statements -> {root}/income_report.parquet",
    ),
    "fina/balance_report": ScopeSpec(
        fetch=balance.fetch_balance,
        date_column=None,
        dedup_keys=("ts_code", "end_date", "ann_date"),
        description="balance sheets -> {root}/balance_report.parquet",
    ),
    "fina/cashflow_report": ScopeSpec(
        fetch=cashflow.fetch_cashflow,
        date_column=None,
        dedup_keys=("ts_code", "end_date", "ann_date"),
        description="cashflow statements -> {root}/cashflow_report.parquet",
    ),
    "fina/consensus": ScopeSpec(
        fetch=consensus.fetch_consensus,
        date_column=None,
        write=consensus.write_consensus,
        description="THS consensus forecasts (akshare) -> {year}/consensus_forecast.parquet "
        "(contract-only in the release catalog)",
    ),
    "fund/portfolio": ScopeSpec(
        fetch=fund_portfolio.fetch_fund_portfolio,
        date_column=None,
        dedup_keys=("fund_code", "end_date", "ts_code"),
        description="fund top holdings -> {root}/fund_portfolio.parquet",
    ),
    # -- events -----------------------------------------------------------------
    "event/forecast": ScopeSpec(
        fetch=forecast.fetch_forecast,
        date_column=None,
        dedup_keys=("ts_code", "ann_date", "end_date"),
        description="performance forecasts -> {root}/forecast.parquet",
    ),
    "event/holdertrade": ScopeSpec(
        fetch=holdertrade.fetch_holdertrade,
        date_column=None,
        dedup_keys=("ts_code", "ann_date", "holder_name", "in_de"),
        description="holder share changes -> {root}/holdertrade.parquet",
    ),
    "event/top10_holder": ScopeSpec(
        fetch=top10_holder.fetch_top10_holder,
        date_column=None,
        dedup_keys=("ts_code", "end_date", "holder_name"),
        description="top-10 float holders -> {root}/top10_holder.parquet",
    ),
    "event/stk_holdernumber": ScopeSpec(
        fetch=stk_holdernumber.fetch_stk_holdernumber,
        date_column="ann_date",
        dedup_keys=("ts_code", "ann_date"),
        description="holder numbers -> {year}/stk_holdernumber.parquet",
    ),
    "event/block_trade": ScopeSpec(
        fetch=block_trade.fetch_block_trade,
        dedup_keys=("ts_code", "trade_date"),
        description="block trades -> {year}/block_trade.parquet",
    ),
    "event/pledge_stat": ScopeSpec(
        fetch=pledge_stat.fetch_pledge_stat,
        dedup_keys=("ts_code", "trade_date"),
        description="pledge statistics -> {year}/pledge_stat.parquet",
    ),
    # -- macro ------------------------------------------------------------------
    "macro/indicator": ScopeSpec(
        fetch=macro.fetch_macro,
        date_column=None,
        dedup_keys=("month", "indicator"),
        description="CPI/PMI/EPU long table -> {root}/macro.parquet",
    ),
}

# legacy_scope strings that no fetcher can serve, with the reason.  The test
# suite asserts mappings stay covered: every scope must be registered here or
# in _SCOPE_FETCHERS.
NON_VENDOR_SCOPES: dict[str, str] = {
    "internal/sample": "strategy-internal control channel, rejected by the external gateway",
    "trade_data/daily 的估值与状态字段": "describes the daily_metric half written by trade_data/daily",
    "kline.parquet + daily_basic.parquet 复合查询": "composite view of market.bar + market.daily_metric",
    "复权因子": "adj_factor column written by trade_data/daily",
    "idx=indicator": "derived technical indicators (tools.indicators), no vendor source",
    "event/equity_incentive": "contract-only mapping, no physical source on the volume",
    "event/private_placement": "contract-only mapping, no physical source on the volume",
    "analyst/rating": "contract-only mapping, no physical source on the volume",
}


def uncovered_legacy_scopes() -> list[str]:
    """Legacy scopes from the mappings that have neither a fetcher nor an exemption."""
    from data.adapters.workspace_data.mappings import MAPPINGS_BY_DATASET

    known = set(_SCOPE_FETCHERS) | set(NON_VENDOR_SCOPES)
    return sorted(
        {
            scope
            for mapping in MAPPINGS_BY_DATASET.values()
            for scope in mapping.legacy_scope
            if scope not in known
        }
    )


class TushareDownloader:
    """Fetch raw vendor frames and land them in the legacy parquet volume.

    Args:
        client: injected vendor SDK client (e.g. ``ts.pro_api(token)``); the
            downloader never reads credentials itself.
        root: raw parquet volume root.
        rate_limit_sleep: seconds between vendor calls (per-scope fetchers
            interpret it as their per-call / per-batch delay).
        trade_dates_provider: optional ``callable(start, end) -> list[str]``
            supplying trading dates; by default the vendor ``trade_cal``
            interface is used.
        scope_options: optional per-scope defaults, e.g.
            ``{"trade_data/minute": {"codes": ["000001.SZ"]}}``.
    """

    def __init__(
        self,
        client: Any = None,
        *,
        root: str | Path = "E:/ProgramData",
        rate_limit_sleep: float = 0.35,
        trade_dates_provider: Callable[[str, str], list[str]] | None = None,
        scope_options: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        if client is None:
            raise ValueError(
                "a vendor client is required (inject it; the downloader never reads credentials)"
            )
        self._client = client
        self._writer = RawVolumeWriter(root)
        self._rate_limit_sleep = rate_limit_sleep
        self._trade_dates_provider = trade_dates_provider
        self._scope_options = {k: dict(v) for k, v in (scope_options or {}).items()}

    @property
    def writer(self) -> RawVolumeWriter:
        return self._writer

    @property
    def scopes(self) -> tuple[str, ...]:
        return tuple(_SCOPE_FETCHERS)

    def fetch(self, scope: str, start: str, end: str, **options: Any) -> Any:
        """Fetch ``scope`` rows for the inclusive YYYYMMDD range (raw frame(s))."""
        spec = _require_spec(scope)
        call_options = {
            "sleep": self._rate_limit_sleep,
            "calendar": self._resolve_calendar,
            "data_root": str(self._writer.root),
            **self._scope_options.get(scope, {}),
            **options,
        }
        return spec.fetch(self._client, start, end, **call_options)

    def download(
        self,
        scope: str,
        start: str,
        end: str,
        *,
        options: Mapping[str, Any] | None = None,
        **write_kwargs: Any,
    ) -> list[Path]:
        """Fetch ``scope`` and land it in the mapped layout; returns touched paths."""
        spec = _require_spec(scope)
        payload = self.fetch(scope, start, end, **(options or {}))
        write_options = {
            "start": start,
            "end": end,
            **self._scope_options.get(scope, {}),
            **(options or {}),
            **write_kwargs,
        }

        if spec.write is not None:
            return list(spec.write(self._writer, payload, **write_options))

        if isinstance(payload, Mapping):
            written: list[Path] = []
            for dataset, frame in payload.items():
                written.extend(
                    self._write_dataset(scope, spec, dataset, frame, **write_kwargs)
                )
            return written
        return self._write_scope(scope, spec, payload, **write_kwargs)

    # -- internals ----------------------------------------------------------

    def _write_scope(self, scope: str, spec: ScopeSpec, frame: pd.DataFrame, **kwargs: Any) -> list[Path]:
        dedup = spec.dedup_keys if isinstance(spec.dedup_keys, Sequence) and not isinstance(spec.dedup_keys, str) else None
        return self._writer.write_scope(
            scope,
            frame,
            date_column=spec.date_column,
            dedup_keys=list(dedup) if dedup else None,
            **kwargs,
        )

    def _write_dataset(
        self, scope: str, spec: ScopeSpec, dataset: str, frame: pd.DataFrame, **kwargs: Any
    ) -> list[Path]:
        dedup: Sequence[str] | None = None
        if isinstance(spec.dedup_keys, Mapping):
            dedup = spec.dedup_keys.get(dataset)
        elif isinstance(spec.dedup_keys, Sequence):
            dedup = spec.dedup_keys
        return self._writer.write_dataset(
            dataset,
            frame,
            date_column=spec.date_column,
            dedup_keys=list(dedup) if dedup else None,
            **kwargs,
        )

    def _resolve_calendar(self, start: str, end: str) -> list[str]:
        if self._trade_dates_provider is not None:
            dates = self._trade_dates_provider(start, end)
            if dates is None:
                raise ValueError(f"trade_dates_provider returned None for {start}~{end}")
            return list(dates)
        if hasattr(self._client, "trade_cal"):
            return calendar.fetch_vendor_trade_dates(self._client, start, end)
        # minimal client without a calendar interface: fall back to naive
        # calendar-day iteration (the legacy skeleton behavior)
        return calendar.naive_calendar_days(start, end)


def _require_spec(scope: str) -> ScopeSpec:
    spec = _SCOPE_FETCHERS.get(scope)
    if spec is None:
        raise NotImplementedError(
            f"scope {scope!r} has no fetcher; registered scopes: {sorted(_SCOPE_FETCHERS)}"
        )
    return spec


__all__ = ["ScopeSpec", "TushareDownloader", "uncovered_legacy_scopes"]
