"""Tushare（及旧版非 Tushare）scope 注册表与下载门面。

注册表将 ``data.adapters.workspace_data.mappings`` 声明的每个旧版 scope
映射到 :class:`ScopeSpec`：

    scope key -> fetcher(client, start, end, **options) -> raw frame(s)

拉取器返回厂商原生数据帧（不做重命名、不做单位转换——规则存在于
``MigrationMapping`` 中）；``TushareDownloader.download`` 通过
``RawVolumeWriter`` 将它们落到映射的物理布局（年度/全局数据用 duckdb 表，
日线与分笔量用按日 parquet）。客户端始终由调用方注入——此处不读取任何
令牌或凭据::

    import tushare as ts
    from config import secrets
    client = ts.pro_api(secrets.TUSHARE_TOKEN)  # token in git-ignored config/secrets.py
    downloader = TushareDownloader(client=client, root="E:/ProgramData")
    downloader.download("trade_data/daily", "20240102", "20241231")

需要交易日的 scope 从注入的 ``trade_dates_provider`` 解析，或默认从厂商
``trade_cal`` 接口本身解析（见 ``scopes.calendar``）。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from data.downloader.base import RawVolumeWriter, dataset_for_scope
from data.downloader.pack import pack_kline
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

# 拉取器接收: (client, start, end, *, sleep, calendar, data_root, **options)
FetchFn = Callable[..., Any]
# 自定义写入器接收: (writer, payload, *, start, end, **options) -> list[Path]
WriteFn = Callable[..., list]
DedupKeys = Sequence[str] | Mapping[str, Sequence[str]]


@dataclass(frozen=True)
class ScopeSpec:
    """注册表条目，将一个旧版 scope 与其拉取 + 写入契约连接起来。

    Attributes:
        fetch: 厂商拉取器，返回原始数据帧、``{dataset_name: frame}``
            字典（多数据集 scope），或 ``None``（完全在 ``write`` 中
            完成工作的 scope）。
        date_column: 用于将行分组成 ``{year}``[/``{MMDD}``] 文件的列；
            全局快照表为 ``None``。
        dedup_keys: 合并去重的业务键（``keep='last'``），可为列元组，
            也可用于多数据集 scope 的按数据集映射。``None`` 表示整行去重。
        write: 可选的自定义写入器，覆盖由映射驱动的写入（用于
            ``MigrationMapping`` 模板无法表达的布局，例如按日的
            ``index_member`` 文件）。
    """

    fetch: FetchFn
    date_column: str | None = "trade_date"
    dedup_keys: DedupKeys | None = None
    write: WriteFn | None = None
    description: str = ""


_SCOPE_FETCHERS: dict[str, ScopeSpec] = {
    # -- 行情 ----------------------------------------------------------------
    "trade_data/daily": ScopeSpec(
        fetch=daily.fetch_daily,
        dedup_keys={
            "market.bar": ("ts_code", "trade_date", "trade_time", "data_type", "timeframe"),
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
        description="intraday bars -> {year}/{MMDD}/kline.parquet, unified kline store with timeframe column",
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
        dedup_keys=("ts_code", "trade_date", "trade_time", "data_type", "timeframe"),
        description="index daily bars -> {year}/kline.parquet (data_type=index)",
    ),
    "fund/daily": ScopeSpec(
        fetch=fund_daily.fetch_fund_daily,
        dedup_keys=("ts_code", "trade_date", "trade_time", "data_type", "timeframe"),
        description="fund daily bars -> {year}/kline.parquet (data_type=fund)",
    ),
    "index/member": ScopeSpec(
        fetch=index_member.fetch_index_member,
        write=index_member.write_index_member,
        description="index constituent weights -> {year}/{MMDD}/index_member.parquet",
    ),
    # -- 参考数据 -------------------------------------------------------------
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
    # -- 基本面 ---------------------------------------------------------------
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
    # -- 事件 ------------------------------------------------------------------
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
    # -- 宏观 ------------------------------------------------------------------
    "macro/indicator": ScopeSpec(
        fetch=macro.fetch_macro,
        date_column=None,
        dedup_keys=("month", "indicator"),
        description="CPI/PMI/EPU long table -> {root}/macro.parquet",
    ),
}

# 没有任何拉取器可服务的 legacy_scope 字符串及其原因。测试套件
# 断言映射保持覆盖：每个 scope 必须在此处或在 _SCOPE_FETCHERS 注册。
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
    """来自映射、既无拉取器也无豁免的旧版 scope。"""
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
    """拉取原始厂商数据帧并落到旧版 parquet 卷。

    Args:
        client: 注入的厂商 SDK 客户端（如 ``ts.pro_api(token)``）；下载器
            自身从不读取凭据。
        root: 原始 parquet 卷根目录。
        rate_limit_sleep: 厂商调用的间隔秒数（各 scope 的拉取器
            将其解释为每次调用 / 每批次的延迟）。
        trade_dates_provider: 可选 ``callable(start, end) -> list[str]``，
            提供交易日；默认使用厂商 ``trade_cal`` 接口。
        scope_options: 可选的按 scope 默认值，如
            ``{"trade_data/minute": {"codes": ["000001.SZ"]}}``。
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
        """拉取 ``scope`` 在闭区间 YYYYMMDD 范围内的行（原始数据帧）。"""
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
        pack: bool = True,
        **write_kwargs: Any,
    ) -> list[Path]:
        """拉取 ``scope`` 并落到映射布局；返回受影响的路径。

        日线类 scope 落成按日 ``kline.parquet`` 文件，随后重新打包
        该年 ``year.duckdb#kline`` 表（1d 行 + 派生的 5d/10d 周期），
        除非 ``pack=False``。
        """
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
            paths = written
        else:
            paths = self._write_scope(scope, spec, payload, **write_kwargs)

        if pack:
            self._pack_kline_years(scope, paths)
        return paths

    # -- 内部方法 ------------------------------------------------------------

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
        # 无日历接口的最小客户端：退化为朴素的
        # 日历日遍历（旧版骨架行为）
        return calendar.naive_calendar_days(start, end)

    def _pack_kline_years(self, scope: str, paths: list[Path]) -> None:
        """在日线类 scope 改动日文件后重新打包年度 kline 表。"""
        if dataset_for_scope(scope) not in ("market.bar", "index.bar", "fund.bar"):
            return
        # 按日文件位于 {year}/{MMDD}/kline.parquet
        years = sorted(
            {path.parent.parent.name for path in paths if path.name == "kline.parquet"}
        )
        for year in years:
            pack_kline(self._writer.root, year)


def _require_spec(scope: str) -> ScopeSpec:
    spec = _SCOPE_FETCHERS.get(scope)
    if spec is None:
        raise NotImplementedError(
            f"scope {scope!r} has no fetcher; registered scopes: {sorted(_SCOPE_FETCHERS)}"
        )
    return spec


__all__ = ["ScopeSpec", "TushareDownloader", "uncovered_legacy_scopes"]
