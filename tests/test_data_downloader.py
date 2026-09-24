"""数据下载包的契约测试。

每个已注册的旧版 scope 都针对注入的假厂商客户端进行测试（无需网络、
无需凭证）：该测试断言：

1. 触达的文件/表精确落在 ``MigrationMapping`` 布局中
   （年度/全局数据用 duckdb 表，每日K线与逐笔卷用按日 parquet），
2. 写入的列集合与旧版布局一致，
3. 重复下载会合并 + 去重（业务键，保留最后一条），
4. 打包后的 ``kline`` 表携带 5d/10d 派生周期，
5. 旧版卷迁移能正确转换、校验并删除。

逐笔 CSV 导入额外断言 ``{year}/{MMDD}/stock.parquet`` 的确切 28 列结构
——包括数据类型——并在真实 USB 卷已挂载时，将产出的结构同真实交易日文件
（``E:/ProgramData/2024/0102/stock.parquet``）进行比较。
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from data.adapters.workspace_data.mappings import MAPPINGS_BY_DATASET
from data.downloader import (
    NON_VENDOR_SCOPES,
    TushareDownloader,
    list_scopes,
    migrate_volume,
    read_duckdb_table,
    uncovered_legacy_scopes,
)

TRADING_DAYS = ["20240102", "20240103"]
INSTRUMENT = "000001.SZ"

REFERENCE_DB = "reference.duckdb"
YEAR_DB = "2024/year.duckdb"


def read_target(root: Path, rel: str, table: str | None) -> pd.DataFrame:
    """读取布局目标：给定表名时读 duckdb 表，否则读 parquet。"""
    if table is not None:
        return read_duckdb_table(root / rel, table)
    return pl.read_parquet(root / rel).to_pandas()


# ---------------------------------------------------------------------------
# 假厂商客户端（离线）
# ---------------------------------------------------------------------------


def _shape(df: pd.DataFrame, fields: str | None) -> pd.DataFrame:
    """模拟 Tushare 的 ``fields`` 投影（按请求顺序，取子集）。"""
    if fields is None:
        return df.copy()
    requested = [c for c in fields.split(",") if c in df.columns]
    return df[requested].copy()


class FakeTushareClient:
    """``ts.pro_api`` 客户端的离线替身：回显厂商布局的数据帧。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def _respond(self, method: str, params: dict, frame: pd.DataFrame) -> pd.DataFrame:
        self.calls.append((method, dict(params)))
        return _shape(frame, params.get("fields"))

    # -- 日历 -----------------------------------------------------------

    def trade_cal(self, exchange="SSE", start_date="", end_date="", fields=None, **kw):
        rows = []
        for day in ("20240101", *TRADING_DAYS, "20240104", "20240105", "20240106", "20240107", "20240108"):
            rows.append(
                {
                    "exchange": exchange,
                    "cal_date": day,
                    "is_open": "1" if day in TRADING_DAYS else "0",
                }
            )
        df = pd.DataFrame(rows)
        df = df[(df["cal_date"] >= start_date) & (df["cal_date"] <= end_date)]
        return self._respond("trade_cal", locals(), df)

    # -- 每日综合 ------------------------------------------------------

    def daily(self, trade_date="", fields=None, **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": INSTRUMENT,
                    "pre_close": 9.21,
                    "trade_date": trade_date,
                    "open": 9.39,
                    "high": 9.42,
                    "low": 9.20,
                    "close": 9.30,
                    "change": 0.09,
                    "pct_chg": 0.98,
                    "vol": 100000.0,
                    "amount": 93000.0,
                }
            ]
        )
        return self._respond("daily", locals(), df)

    def bak_basic(self, trade_date="", fields=None, **kw):
        df = pd.DataFrame([{"ts_code": INSTRUMENT, "trade_date": trade_date, "name": "平安银行"}])
        return self._respond("bak_basic", locals(), df)

    def adj_factor(self, trade_date="", fields=None, **kw):
        df = pd.DataFrame([{"ts_code": INSTRUMENT, "adj_factor": 12.34}])
        return self._respond("adj_factor", locals(), df)

    def stock_st(self, trade_date="", fields=None, **kw):
        return self._respond("stock_st", locals(), pd.DataFrame(columns=["ts_code"]))

    def suspend_d(self, suspend_type="", trade_date="", fields=None, **kw):
        return self._respond("suspend_d", locals(), pd.DataFrame(columns=["ts_code"]))

    def limit_list_d(self, trade_date="", limit_type="", fields=None, **kw):
        df = pd.DataFrame([{"ts_code": INSTRUMENT, "trade_date": trade_date, "limit": "U"}])
        return self._respond("limit_list_d", locals(), df)

    def stk_limit(self, trade_date="", fields=None, **kw):
        df = pd.DataFrame([{"ts_code": INSTRUMENT, "up_limit": 10.23, "down_limit": 8.37}])
        return self._respond("stk_limit", locals(), df)

    def daily_basic(self, trade_date="", fields=None, **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": INSTRUMENT,
                    "turnover_rate_f": 0.8,
                    "volume_ratio": 0.9,
                    "pe_ttm": 5.1,
                    "pb": 0.5,
                    "ps_ttm": 0.6,
                    "dv_ttm": 5.2,
                    "total_share": 10000.0,
                    "float_share": 9000.0,
                    "free_share": 8000.0,
                    "total_mv": 93000.0,
                    "circ_mv": 83700.0,
                }
            ]
        )
        return self._respond("daily_basic", locals(), df)

    # -- K线 ---------------------------------------------------------------

    def index_daily(self, ts_code="", start_date="", end_date="", **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": ts_code,
                    "trade_date": day,
                    "open": 3000.0,
                    "high": 3050.0,
                    "low": 2990.0,
                    "close": 3020.0,
                    "pre_close": 3010.0,
                    "change": 10.0,
                    "pct_chg": 0.33,
                    "vol": 1000.0,
                    "amount": 302000.0,
                }
                for day in TRADING_DAYS
            ]
        )
        return self._respond("index_daily", locals(), df)

    def fund_daily(self, ts_code="", start_date="", end_date="", fields=None, **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": ts_code,
                    "trade_date": day,
                    "pre_close": 4.0,
                    "open": 4.01,
                    "high": 4.05,
                    "low": 3.99,
                    "close": 4.02,
                    "pct_chg": 0.5,
                    "vol": 50000.0,
                    "amount": 20100.0,
                }
                for day in TRADING_DAYS
            ]
        )
        return self._respond("fund_daily", locals(), df)

    def stk_mins(self, ts_code="", freq="30min", start_date="", end_date="", **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": ts_code,
                    "trade_time": "2024-01-02 09:30:00",
                    "open": 9.39,
                    "high": 9.40,
                    "low": 9.38,
                    "close": 9.39,
                    "vol": 100.0,
                }
            ]
        )
        return self._respond("stk_mins", locals(), df)

    # -- 资金流向 / 市场状态 --------------------------------------------------

    def moneyflow(self, trade_date="", **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": INSTRUMENT,
                    "trade_date": trade_date,
                    "buy_sm_vol": 100.0,
                    "buy_sm_amount": 930.0,
                    "sell_sm_vol": 90.0,
                    "sell_sm_amount": 837.0,
                    "buy_md_vol": 50.0,
                    "buy_md_amount": 465.0,
                    "sell_md_vol": 40.0,
                    "sell_md_amount": 372.0,
                    "buy_lg_vol": 20.0,
                    "buy_lg_amount": 186.0,
                    "sell_lg_vol": 10.0,
                    "sell_lg_amount": 93.0,
                    "buy_elg_vol": 5.0,
                    "buy_elg_amount": 46.5,
                    "sell_elg_vol": 2.0,
                    "sell_elg_amount": 18.6,
                    "net_mf_vol": 33.0,
                    "net_mf_amount": 306.9,
                }
            ]
        )
        return self._respond("moneyflow", locals(), df)

    def margin(self, start_date="", end_date="", **kw):
        df = pd.DataFrame(
            [
                {
                    "trade_date": day,
                    "rzmre": 100.0,
                    "rzye": 200.0,
                    "rqye": 300.0,
                    "rzrqye": 500.0,
                }
                for day in TRADING_DAYS
            ]
        )
        return self._respond("margin", locals(), df)

    def margin_detail(self, start_date="", end_date="", offset=0, **kw):
        df = pd.DataFrame(
            [
                {
                    "trade_date": day,
                    "ts_code": INSTRUMENT,
                    "rzymye": 150.0,
                    "rzmre": 100.0,
                    "rzche": 50.0,
                    "rqmcl": 10.0,
                    "rqchl": 5.0,
                    "rqye": 300.0,
                    "rzrqye": 450.0,
                }
                for day in TRADING_DAYS
            ]
        )
        return self._respond("margin_detail", locals(), df)

    def moneyflow_hsgt(self, start_date="", end_date="", **kw):
        df = pd.DataFrame(
            [
                {
                    "trade_date": day,
                    "ggt_ss": 1.0,
                    "ggt_sz": 2.0,
                    "hgt": 3.0,
                    "sgt": 4.0,
                    "north_money": 7.0,
                    "south_money": -7.0,
                }
                for day in TRADING_DAYS
            ]
        )
        return self._respond("moneyflow_hsgt", locals(), df)

    def shibor(self, start_date="", end_date="", **kw):
        df = pd.DataFrame(
            [
                {
                    "date": day,
                    "on": 1.5,
                    "1w": 1.7,
                    "2w": 1.8,
                    "1m": 1.9,
                    "3m": 2.0,
                    "6m": 2.1,
                    "9m": 2.2,
                    "1y": 2.3,
                }
                for day in TRADING_DAYS
            ]
        )
        return self._respond("shibor", locals(), df)

    def yc_cb(self, ts_code="", curve_type=0, start_date="", end_date="", **kw):
        df = pd.DataFrame(
            [
                {
                    "trade_date": day,
                    "ts_code": ts_code,
                    "curve_name": "中债国债收益率曲线",
                    "curve_type": curve_type,
                    "curve_term": term,
                    "yield": 2.1 + term / 100,
                }
                for day in TRADING_DAYS
                for term in (5.0, 10.0)
            ]
        )
        return self._respond("yc_cb", locals(), df)

    # -- 参考 ------------------------------------------------------------

    def stock_basic(self, list_status="L", fields=None, exchange="", **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": INSTRUMENT,
                    "symbol": "000001",
                    "name": "平安银行",
                    "area": "深圳",
                    "industry": "银行",
                    "fullname": "平安银行股份有限公司",
                    "market": "主板",
                    "exchange": "SZSE",
                    "list_status": "L",
                    "list_date": "19910403",
                    "delist_date": None,
                    "is_hs": "S",
                },
                {
                    "ts_code": "600000.SH",
                    "symbol": "600000",
                    "name": "浦发银行",
                    "area": "上海",
                    "industry": "银行",
                    "fullname": "上海浦东发展银行股份有限公司",
                    "market": "主板",
                    "exchange": "SSE",
                    "list_status": "L",
                    "list_date": "19991110",
                    "delist_date": None,
                    "is_hs": "H",
                },
            ]
        )
        return self._respond("stock_basic", locals(), df)

    def index_member(self, index_code="", **kw):
        df = pd.DataFrame(
            [
                {
                    "index_code": index_code,
                    "con_code": INSTRUMENT,
                    "in_date": "20211213",
                    "out_date": None,
                    "is_new": "Y",
                }
            ]
        )
        return self._respond("index_member", locals(), df)

    def index_weight(self, index_code="", start_date="", end_date="", **kw):
        df = pd.DataFrame(
            [
                {
                    "index_code": index_code,
                    "con_code": INSTRUMENT,
                    "trade_date": day,
                    "weight": 0.85,
                }
                for day in TRADING_DAYS
            ]
        )
        return self._respond("index_weight", locals(), df)

    # -- 基本面 / 事件 ------------------------------------------------

    def _report(self, method: str, period: str, fields: str | None, extra: dict):
        base = {"ts_code": INSTRUMENT, "end_date": period, "ann_date": "20240415"}
        df = pd.DataFrame([{**base, **extra}])
        return self._respond(method, {**locals(), "fields": fields}, df)

    def fina_indicator_vip(self, period="", fields=None, **kw):
        return self._report(
            "fina_indicator_vip",
            period,
            fields,
            {"roe": 10.0, "eps": 1.5, "or_yoy": 8.8},
        )

    def income_vip(self, period="", fields=None, **kw):
        return self._report("income_vip", period, fields, {"revenue": 100.0, "n_income": 20.0})

    def balancesheet_vip(self, period="", fields=None, **kw):
        return self._report("balancesheet_vip", period, fields, {"total_assets": 500.0, "total_liab": 300.0})

    def cashflow_vip(self, period="", fields=None, **kw):
        return self._report("cashflow_vip", period, fields, {"net_profit": 20.0, "n_cashflow_act": 15.0})

    def fund_portfolio(self, period="", offset=0, limit=8000, fields=None, **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": "001234.OF",
                    "ann_date": "20240422",
                    "end_date": period,
                    "symbol": INSTRUMENT,
                    "mkv": 1200.0,
                    "amount": 100.0,
                    "stk_mkv_ratio": 5.0,
                    "stk_float_ratio": 0.1,
                }
            ]
        )
        return self._respond("fund_portfolio", locals(), df)

    def forecast(self, ann_date="", fields=None, **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": INSTRUMENT,
                    "ann_date": ann_date,
                    "end_date": "20231231",
                    "type": "预减",
                    "p_change_min": -50.0,
                    "p_change_max": -20.0,
                    "net_profit_min": 100.0,
                    "net_profit_max": 200.0,
                },
                {
                    "ts_code": "600000.SH",
                    "ann_date": ann_date,
                    "end_date": "20231231",
                    "type": "扭亏",
                    "p_change_min": 100.0,
                    "p_change_max": 150.0,
                    "net_profit_min": 300.0,
                    "net_profit_max": 400.0,
                },
                {
                    "ts_code": "600000.SH",
                    "ann_date": ann_date,
                    "end_date": "20231230",
                    "type": "略增",  # 必须被旧版行过滤器过滤掉
                    "p_change_min": 10.0,
                    "p_change_max": 30.0,
                    "net_profit_min": 1.0,
                    "net_profit_max": 2.0,
                },
            ]
        )
        return self._respond("forecast", locals(), df)

    def stk_holdertrade(self, start_date="", end_date="", fields=None, **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": INSTRUMENT,
                    "ann_date": day,
                    "holder_name": "某股东",
                    "holder_type": "G",
                    "in_de": "IN",
                    "change_vol": 100.0,
                    "change_ratio": 0.1,
                    "after_share": 1000.0,
                    "after_ratio": 5.0,
                    "avg_price": 9.3,
                    "total_share": 20000.0,
                    "begin_date": day,
                    "close_date": day,
                }
                for day in TRADING_DAYS
            ]
        )
        return self._respond("stk_holdertrade", locals(), df)

    def top10_floatholders(self, ts_code="", end_date="", fields=None, **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": ts_code,
                    "ann_date": "20240420",
                    "end_date": end_date,
                    "holder_name": "香港中央结算有限公司",
                    "hold_amount": 500.0,
                    "hold_ratio": 2.5,
                }
            ]
        )
        return self._respond("top10_floatholders", locals(), df)

    def stk_holdernumber(self, start_date="", end_date="", **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": INSTRUMENT,
                    "ann_date": day,
                    "end_date": "20231231",
                    "holder_num": 50000,
                }
                for day in TRADING_DAYS
            ]
        )
        return self._respond("stk_holdernumber", locals(), df)

    def block_trade(self, trade_date="", **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": INSTRUMENT,
                    "trade_date": trade_date,
                    "price": 9.3,
                    "vol": 100.0,
                    "amount": 930.0,
                    "buyer": "机构专用",
                    "seller": "机构专用",
                }
            ]
        )
        return self._respond("block_trade", locals(), df)

    def pledge_stat(self, trade_date="", **kw):
        df = pd.DataFrame(
            [
                {
                    "ts_code": INSTRUMENT,
                    "end_date": "20231231",
                    "pledge_count": 2,
                    "unrest_pledge": 1,
                    "rest_pledge": 1,
                    "total_share": 100.0,
                    "pledge_ratio": 5.0,
                }
            ]
        )
        return self._respond("pledge_stat", locals(), df)

    # -- 宏观 ----------------------------------------------------------------

    def cn_cpi(self, start_month="", end_month="", **kw):
        return self._respond(
            "cn_cpi",
            locals(),
            pd.DataFrame([{"month": m, "nt_yoy": 0.5} for m in ("202312", "202401")]),
        )

    def cn_pmi(self, start_month="", end_month="", **kw):
        return self._respond(
            "cn_pmi",
            locals(),
            pd.DataFrame([{"MONTH": m, "PMI010000": 49.5} for m in ("202312", "202401")]),
        )


class FakeAkshareNorthbound:
    """北向 scope 用到的 akshare 函数的离线替身。"""

    def stock_info_a_code_name(self):
        return pd.DataFrame({"code": ["000001"], "name": ["平安银行"]})

    def stock_hsgt_individual_em(self, symbol=""):
        return pd.DataFrame(
            [
                {
                    "持股日期": "2024-01-02",
                    "今日增持股数": 1000.0,
                    "今日增持资金": 9290.0,
                    "持股数量": 20000.0,
                    "持股市值": 185800.0,
                    "当日收盘价": 9.29,
                    "当日涨跌幅": 0.98,
                }
            ]
        )


class FakeAkshareConsensus:
    def stock_info_a_code_name(self):
        return pd.DataFrame({"code": ["000001"], "name": ["平安银行"]})

    def stock_profit_forecast_ths(self, symbol="", indicator=""):
        return pd.DataFrame(
            {
                "报告期": [2024, 2025],
                "实际值": [None, None],
                "预测均值": [1.5, 1.8],
            }
        )


# ---------------------------------------------------------------------------
# 夹具
# ---------------------------------------------------------------------------


@pytest.fixture()
def root(tmp_path) -> Path:
    return tmp_path / "ProgramData"


@pytest.fixture()
def client() -> FakeTushareClient:
    return FakeTushareClient()


@pytest.fixture()
def downloader(client, root) -> TushareDownloader:
    return TushareDownloader(client=client, root=root, rate_limit_sleep=0.0)


# ---------------------------------------------------------------------------
# 注册表覆盖
# ---------------------------------------------------------------------------


def test_registry_covers_all_mapping_legacy_scopes():
    """每个 mappings.py 的 legacy_scope 都已注册或显式豁免。"""
    assert uncovered_legacy_scopes() == []
    registered = set(list_scopes())
    mapped = {
        scope
        for mapping in MAPPINGS_BY_DATASET.values()
        for scope in mapping.legacy_scope
    }
    assert mapped <= registered | set(NON_VENDOR_SCOPES)
    # trade_data/minute 在映射之外也有注册（分钟K线存放在发布目录的
    # 按日逐笔卷中）——多出的注册没有问题。


def test_downloader_requires_injected_client():
    with pytest.raises(ValueError, match="inject"):
        TushareDownloader(client=None)
    with pytest.raises(ValueError, match="inject"):
        TushareDownloader()


def test_unknown_scope_raises(downloader):
    with pytest.raises(NotImplementedError, match="no fetcher"):
        downloader.download("trade_data/unknown", "20240102", "20240103")


def test_fetcher_never_reads_credentials(downloader, client):
    """下载器只与注入的客户端接口交互。"""
    downloader.download("stock/basic", "20240102", "20240103")
    methods = {method for method, _ in client.calls}
    assert methods == {"stock_basic"}


# ---------------------------------------------------------------------------
# 逐 scope 布局契约（假客户端，离线）
# ---------------------------------------------------------------------------

# （scope，options，{(相对目标, 表名或None): 期望列}）
SCOPE_CASES = [
    pytest.param(
        "calendar/trade_cal",
        {},
        {(REFERENCE_DB, "trade_date"): ["cal_date", "is_open"]},
        id="calendar/trade_cal",
    ),
    pytest.param(
        "stock/basic",
        {},
        {(REFERENCE_DB, "stock_basic"): None},  # 列 == 请求的厂商字段
        id="stock/basic",
    ),
    pytest.param(
        "sw/industry",
        {},
        {(REFERENCE_DB, "sw_industry"): ["index_code", "con_code", "in_date", "out_date", "is_new"]},
        id="sw/industry",
    ),
    pytest.param(
        "trade_data/moneyflow",
        {},
        {(YEAR_DB, "moneyflow"): None},
        id="trade_data/moneyflow",
    ),
    pytest.param(
        "idx=daily_margin",
        {},
        {(YEAR_DB, "margin"): ["trade_date", "rzmre", "rzye", "rqye", "rzrqye", "level", "ts_code"]},
        id="idx=daily_margin",
    ),
    pytest.param(
        "trade_data/margin_detail",
        {},
        {(YEAR_DB, "margin"): ["trade_date", "ts_code", "rzymye", "rzmre", "rzche", "rqmcl", "rqchl", "rqye", "rzrqye", "level"]},
        id="trade_data/margin_detail",
    ),
    pytest.param(
        "trade_data/northbound",
        {"akshare": None},  # 用下方的假客户端替换
        {(YEAR_DB, "northbound_netbuy"): [
            "trade_date", "net_buy_shares", "net_buy", "hold_shares",
            "hold_market_cap", "close", "pct_chg", "ts_code",
        ]},
        id="trade_data/northbound",
    ),
    pytest.param(
        "trade_data/moneyflow_hsgt",
        {},
        {(YEAR_DB, "moneyflow_hsgt"): ["trade_date", "ggt_ss", "ggt_sz", "hgt", "sgt", "north_money", "south_money"]},
        id="trade_data/moneyflow_hsgt",
    ),
    pytest.param(
        "trade_data/shibor",
        {},
        {(YEAR_DB, "shibor"): ["date", "on", "1w", "2w", "1m", "3m", "6m", "9m", "1y"]},
        id="trade_data/shibor",
    ),
    pytest.param(
        "trade_data/yc_cb",
        {},
        {(YEAR_DB, "yc_cb"): ["trade_date", "ts_code", "curve_name", "curve_type", "curve_term", "yield"]},
        id="trade_data/yc_cb",
    ),
    pytest.param(
        "index/daily",
        {},
        {("2024/0102/kline.parquet", None): None, (YEAR_DB, "kline"): None},
        id="index/daily",
    ),
    pytest.param(
        "fund/daily",
        {},
        {("2024/0102/kline.parquet", None): None, (YEAR_DB, "kline"): None},
        id="fund/daily",
    ),
    pytest.param(
        "index/member",
        {},
        {(YEAR_DB, "index_member"): ["index_code", "con_code", "trade_date", "weight"]},
        id="index/member",
    ),
    pytest.param(
        "fina/indicator",
        {},
        {(REFERENCE_DB, "fina_indicator"): None},
        id="fina/indicator",
    ),
    pytest.param(
        "fina/income_report",
        {},
        {(REFERENCE_DB, "income_report"): None},
        id="fina/income_report",
    ),
    pytest.param(
        "fina/balance_report",
        {},
        {(REFERENCE_DB, "balance_report"): None},
        id="fina/balance_report",
    ),
    pytest.param(
        "fina/cashflow_report",
        {},
        {(REFERENCE_DB, "cashflow_report"): None},
        id="fina/cashflow_report",
    ),
    pytest.param(
        "fund/portfolio",
        {},
        {(REFERENCE_DB, "fund_portfolio"): ["fund_code", "ann_date", "end_date", "ts_code", "mkv", "amount", "stk_mkv_ratio", "stk_float_ratio"]},
        id="fund/portfolio",
    ),
    pytest.param(
        "event/forecast",
        {},
        {(REFERENCE_DB, "forecast"): ["ts_code", "ann_date", "end_date", "type", "p_change_min", "p_change_max", "net_profit_min", "net_profit_max"]},
        id="event/forecast",
    ),
    pytest.param(
        "event/holdertrade",
        {},
        {(REFERENCE_DB, "holdertrade"): None},
        id="event/holdertrade",
    ),
    pytest.param(
        "event/top10_holder",
        {},
        {(REFERENCE_DB, "top10_holder"): ["ts_code", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio"]},
        id="event/top10_holder",
    ),
    pytest.param(
        "event/stk_holdernumber",
        {},
        {(YEAR_DB, "stk_holdernumber"): ["ts_code", "ann_date", "end_date", "holder_num"]},
        id="event/stk_holdernumber",
    ),
    pytest.param(
        "event/block_trade",
        {},
        {(YEAR_DB, "block_trade"): ["ts_code", "trade_date", "price", "vol", "amount", "buyer", "seller"]},
        id="event/block_trade",
    ),
    pytest.param(
        "event/pledge_stat",
        {},
        {(YEAR_DB, "pledge_stat"): ["ts_code", "end_date", "pledge_count", "unrest_pledge", "rest_pledge", "total_share", "pledge_ratio", "trade_date"]},
        id="event/pledge_stat",
    ),
    pytest.param(
        "macro/indicator",
        {"epu_fetcher": None},  # 用下方的假客户端替换
        {(REFERENCE_DB, "macro"): ["month", "indicator", "value"]},
        id="macro/indicator",
    ),
    pytest.param(
        "fina/consensus",
        {"akshare": None},  # 用下方的假客户端替换
        {(YEAR_DB, "consensus_forecast"): ["ts_code", "year", "eps_mean", "np_mean"]},
        id="fina/consensus",
    ),
]

START, END = "20240101", "20241231"


def _resolve_scope_options(scope: str, options: dict) -> dict:
    """将占位注入替换为离线假客户端，并关闭延迟。"""
    options = dict(options)
    if scope == "trade_data/northbound":
        options["akshare"] = FakeAkshareNorthbound()
        options["sleep"] = 0.0
    if scope == "fina/consensus":
        options["akshare"] = FakeAkshareConsensus()
        options["sleep"] = 0.0
    if scope == "macro/indicator":
        options["epu_fetcher"] = lambda: pd.DataFrame(
            {"year": [2024], "month": [1], "China_Policy_Index": [350.0]}
        )
    return options


@pytest.mark.parametrize("scope,options,expected", SCOPE_CASES)
def test_scope_download_lands_mapping_layout(downloader, client, root, scope, options, expected):
    options = _resolve_scope_options(scope, options)

    paths = downloader.download(scope, START, END, options=options)

    assert paths, f"scope {scope} wrote nothing"
    returned = {str(p.relative_to(root)) for p in paths}
    for (rel, table), columns in expected.items():
        target = root / rel
        assert target.is_file(), f"scope {scope} missing {rel}"
        if table is None:
            # parquet 目标直接写入，且必须出现在返回的路径中
            assert str(target.relative_to(root)) in returned, f"scope {scope}: {rel} not returned"
        frame = read_target(root, rel, table)
        assert len(frame) > 0
        if columns is not None:
            assert list(frame.columns) == columns, f"scope {scope}: {rel}#{table} columns"


@pytest.mark.parametrize("scope,options,expected", SCOPE_CASES)
def test_scope_download_is_idempotent(downloader, root, scope, options, expected):
    """重复运行同一下载不会产生重复行。"""
    options = _resolve_scope_options(scope, options)

    downloader.download(scope, START, END, options=options)
    counts_before = {
        (rel, table): len(read_target(root, rel, table)) for rel, table in expected
    }
    downloader.download(scope, START, END, options=options)
    for target_key, count in counts_before.items():
        rel, table = target_key
        assert len(read_target(root, rel, table)) == count, f"scope {scope}: {rel}#{table} duplicated rows"


# ---------------------------------------------------------------------------
# 多数据集 scope 与 kline 打包
# ---------------------------------------------------------------------------


def test_daily_scope_writes_day_files_and_packed_tables(downloader, root):
    paths = downloader.download("trade_data/daily", START, END)

    # 每日K线文件是原始层；daily_metric 表与年度库共用
    assert {p.relative_to(root) for p in paths} == {
        Path("2024") / "0102" / "kline.parquet",
        Path("2024") / "0103" / "kline.parquet",
        Path(YEAR_DB),
    }
    day_frame = pl.read_parquet(root / "2024" / "0102" / "kline.parquet").to_pandas()
    assert list(day_frame.columns) == [
        "ts_code", "trade_date", "open", "high", "low", "close", "pre_close",
        "pct_chg", "vol", "amount", "adj_factor", "name", "data_type", "timeframe",
        "change",
    ]
    assert set(day_frame["data_type"]) == {"stock"}

    # 打包后的年度 kline 表（自动打包）+ 每日指标表
    kline = read_duckdb_table(root / YEAR_DB, "kline")
    assert len(kline[kline["timeframe"] == "1d"]) == len(TRADING_DAYS)
    basic = read_duckdb_table(root / YEAR_DB, "daily_basic")
    assert list(basic.columns) == [
        "ts_code", "trade_date", "is_st", "is_suspended", "turnover_rate_f",
        "volume_ratio", "pe_ttm", "pb", "ps_ttm", "dv_ttm", "total_share",
        "float_share", "free_share", "total_mv", "circ_mv", "up_limit",
        "down_limit", "limit",
    ]
    assert len(basic) == len(TRADING_DAYS)
    assert basic["limit"][0] == "U"


def test_kline_pack_derives_multi_day_timeframes(downloader, root):
    downloader.download("trade_data/daily", START, END)
    kline = read_duckdb_table(root / YEAR_DB, "kline")

    blocks = kline[kline["timeframe"] == "5d"]
    assert len(blocks) == 1  # 2 个交易日组成一个 5 日块
    block = blocks.iloc[0]
    assert block["trade_date"] == "20240103"  # 块在最后一天结束
    assert block["open"] == 9.39 and block["close"] == 9.30
    assert block["vol"] == 200000.0 and block["amount"] == 186000.0
    assert block["pre_close"] is None or pd.isna(block["pre_close"])  # 没有更早的块

    ten_day = kline[kline["timeframe"] == "10d"]
    assert len(ten_day) == 1


def test_daily_repeated_download_merges_and_refreshes(downloader, root):
    downloader.download("trade_data/daily", START, END)

    # 第二次运行使用变更后的收盘价，应刷新该行而非重复
    original = downloader._client.daily

    def shifted(**params):
        frame = original(**params)
        frame["close"] = 9.99
        return frame

    downloader._client.daily = shifted
    downloader.download("trade_data/daily", START, END)

    day_frame = pl.read_parquet(root / "2024" / "0102" / "kline.parquet").to_pandas()
    assert len(day_frame) == 1  # 每个文件一天
    assert set(day_frame["close"]) == {9.99}
    kline = read_duckdb_table(root / YEAR_DB, "kline")
    base = kline[kline["timeframe"] == "1d"]
    assert len(base) == len(TRADING_DAYS)
    assert set(base["close"]) == {9.99}


def test_margin_summary_and_detail_share_the_year_table(downloader, root):
    downloader.download("idx=daily_margin", START, END)
    downloader.download("trade_data/margin_detail", START, END)

    margin = read_duckdb_table(root / YEAR_DB, "margin")
    assert set(margin["level"]) == {"summary", "detail"}
    assert len(margin) == len(TRADING_DAYS) * 2
    summary = margin[margin["level"] == "summary"]
    assert summary["ts_code"].isna().sum() == len(TRADING_DAYS)


def test_index_and_fund_bars_share_the_kline_layout(downloader, root):
    downloader.download("trade_data/daily", START, END)
    downloader.download("index/daily", START, END)
    downloader.download("fund/daily", START, END)

    day_frame = pl.read_parquet(root / "2024" / "0102" / "kline.parquet").to_pandas()
    assert set(day_frame["data_type"]) == {"stock", "index", "fund"}
    kline = read_duckdb_table(root / YEAR_DB, "kline")
    base = kline[kline["timeframe"] == "1d"]
    assert len(base) == len(base.drop_duplicates(subset=["ts_code", "trade_date", "data_type", "timeframe"]))


def test_breadth_is_aggregated_from_the_day_files(downloader, root):
    assert downloader.download("trade_data/daily", START, END)

    paths = downloader.download("trade_data/breadth", START, END)
    assert {p.relative_to(root) for p in paths} == {Path(YEAR_DB)}

    breadth = read_duckdb_table(root / YEAR_DB, "market_breadth")
    assert list(breadth.columns) == ["trade_date", "up_amount", "down_amount", "total_amount"]
    assert len(breadth) == len(TRADING_DAYS)


def test_minute_scope_writes_freq_table(downloader, root):
    paths = downloader.download(
        "trade_data/minute",
        START,
        END,
        options={"codes": [INSTRUMENT], "freq": "30min"},
    )
    assert [p.relative_to(root) for p in paths] == [Path(YEAR_DB)]
    frame = read_duckdb_table(root / YEAR_DB, "minute_30min")
    assert {"ts_code", "trade_time"} <= set(frame.columns)


def test_minute_scope_requires_codes(downloader):
    """缺少 codes 选项时，fetcher 会发出警告且不写入任何内容。"""
    assert downloader.download("trade_data/minute", START, END) == []


def test_global_snapshot_merge_dedupe(downloader, root):
    downloader.download("calendar/trade_cal", START, END)
    first = read_duckdb_table(root / REFERENCE_DB, "trade_date")
    downloader.download("calendar/trade_cal", START, END)
    second = read_duckdb_table(root / REFERENCE_DB, "trade_date")
    assert len(second) == len(first)  # 按 cal_date 去重


def test_forecast_scope_keeps_legacy_row_filter(downloader, root):
    downloader.download("event/forecast", START, END)
    forecast = read_duckdb_table(root / REFERENCE_DB, "forecast")
    assert set(forecast["type"]) == {"预减", "扭亏"}  # "略增" 会被过滤，同旧版卷


def test_trade_cal_scope_feeds_downstream_calendar(downloader, root):
    """下载日历后，依赖日历的 range 类 download 便可解析交易日。"""
    downloader.download("calendar/trade_cal", START, END)

    def provider(start, end):
        table = read_duckdb_table(root / REFERENCE_DB, "trade_date")
        table["cal_date"] = table["cal_date"].astype(str)
        rows = table[(table["is_open"] == "1") & (table["cal_date"] >= start) & (table["cal_date"] <= end)]
        return sorted(rows["cal_date"].tolist())

    local = TushareDownloader(
        client=downloader._client,
        root=root,
        rate_limit_sleep=0.0,
        trade_dates_provider=provider,
    )
    paths = local.download("event/block_trade", START, END)
    assert [p.relative_to(root) for p in paths] == [Path(YEAR_DB)]


# ---------------------------------------------------------------------------
# 逐笔 CSV 导入
# ---------------------------------------------------------------------------

CSV_HEADER = (
    "日期,时间,成交价,成交量,总量,额,"
    "B1价,B1量,B2价,B2量,B3价,B3量,B4价,B4量,B5价,B5量,"
    "S1价,S1量,S2价,S2量,S3价,S3量,S4价,S4量,S5价,S5量,BS"
)


def _tick_row(time, pr, vol, total_vol, amount, bs="B", price2="9.30"):
    return (
        f"20240102,{time},{pr},{vol},{total_vol},{amount},"
        f"{price2},100,9.31,100,9.32,100,9.33,100,9.34,100,"
        f"9.35,100,9.36,100,9.37,100,9.38,100,9.39,100,{bs}"
    )


def _write_csv(path: Path, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="gbk", newline="") as stream:
        stream.write(CSV_HEADER + "\n")
        for row in rows:
            stream.write(row + "\n")


def _build_csv_volume(csv_root: Path) -> None:
    """SH + SZ 月度目录：股票、一个指数、一个 ETF，以及被排除的 B 股。"""
    stock_rows = [
        _tick_row("09:15:00", "0.0", "0", "0", "0"),          # 开盘集合竞价 -> flag 0
        _tick_row("09:30:00", "9.29", "10", "10", "9290"),    # 连续竞价 -> flag 1
        _tick_row("14:57:00", "9.21", "0", "100", "9210"),    # 收盘集合竞价 -> flag 2
    ]
    index_rows = [
        "20240102,09:30:00,3020.5,100,200,302050000",
        "20240102,09:30:05,3021.0,50,250,302150000",
    ]
    _write_csv(csv_root / "SH" / "202401" / "20240102" / "600000.csv", stock_rows)
    _write_csv(csv_root / "SH" / "202401" / "20240102" / "000001.csv", index_rows)  # SH 000xxx -> 指数
    _write_csv(csv_root / "SZ" / "202401" / "20240102" / "000001.csv", stock_rows)  # SZ 000xxx -> 股票
    _write_csv(csv_root / "SZ" / "202401" / "20240102" / "510300.csv", stock_rows)  # ETF -> 可交易
    _write_csv(csv_root / "SZ" / "202401" / "20240102" / "900001.csv", stock_rows)  # B 股 -> 被排除


def test_tick_import_routes_and_matches_legacy_schemas(tmp_path):
    csv_root = tmp_path / "csv"
    root = tmp_path / "ProgramData"
    _build_csv_volume(csv_root)

    from data.downloader.tick_import import import_date_range

    imported = import_date_range(csv_root, root, start="20240102", end="20240102")
    assert imported == 4  # B 股 CSV 会被排除

    day_dir = root / "2024" / "0102"
    stock = pl.read_parquet(day_dir / "stock.parquet")
    tradable = pl.read_parquet(day_dir / "tradable.parquet")
    index = pl.read_parquet(day_dir / "index.parquet")

    # stock：按顺序的确切 28 列旧版布局
    assert list(stock.columns) == [
        "time", "pr", "vol", "total_vol", "amount",
        "b1p", "b1v", "b2p", "b2v", "b3p", "b3v", "b4p", "b4v", "b5p", "b5v",
        "s1p", "s1v", "s2p", "s2v", "s3p", "s3v", "s4p", "s4v", "s5p", "s5v",
        "bs", "code", "flag",
    ]
    schema = dict(zip(stock.columns, stock.dtypes))
    assert schema["time"] == pl.String and schema["code"] == pl.String and schema["bs"] == pl.String
    for col in ("pr", "b1p", "b5p", "s1p", "s5p"):
        assert schema[col] == pl.Float64, col
    for col in ("vol", "total_vol", "amount", "b1v", "b5v", "s1v", "s5v"):
        assert schema[col] == pl.Int64, col
    assert schema["flag"] == pl.Int64

    # 厂商单位原样保留（无 x100，无元转换）
    row = stock.filter(pl.col("time") == "09:30:00")
    assert row["vol"][0] == 10
    assert row["total_vol"][0] == 10
    assert row["amount"][0] == 9290

    # flag 语义：0 开盘集合竞价，1 连续竞价，2 收盘集合竞价
    assert stock.filter(pl.col("time") == "09:15:00")["flag"][0] == 0
    assert stock.filter(pl.col("time") == "09:30:00")["flag"][0] == 1
    assert stock.filter(pl.col("time") == "14:57:00")["flag"][0] == 2

    # tradable：同样的 28 列，数值列全为 Float64，flag 为 Int32
    assert list(tradable.columns) == list(stock.columns)
    t_schema = dict(zip(tradable.columns, tradable.dtypes))
    for col in ("pr", "vol", "amount", "b1v", "s5v"):
        assert t_schema[col] == pl.Float64, col
    assert t_schema["flag"] == pl.Int32
    assert set(tradable["code"]) == {"510300.SZ"}

    # index：6 列，code 在最前，数值列全为 Float64
    assert list(index.columns) == ["code", "time", "pr", "vol", "total_vol", "amount"]
    i_schema = dict(zip(index.columns, index.dtypes))
    assert all(i_schema[c] == pl.Float64 for c in ("pr", "vol", "total_vol", "amount"))
    assert set(index["code"]) == {"000001.SH"}


def test_tick_import_append_dedupes_on_code_and_time(tmp_path):
    csv_root = tmp_path / "csv"
    root = tmp_path / "ProgramData"
    _build_csv_volume(csv_root)

    from data.downloader.tick_import import import_date_range

    import_date_range(csv_root, root, start="20240102", end="20240102")
    stock_path = root / "2024" / "0102" / "stock.parquet"
    first = pl.read_parquet(stock_path)

    import_date_range(csv_root, root, start="20240102", end="20240102")
    second = pl.read_parquet(stock_path)
    assert second.height == first.height


def test_tick_scope_via_downloader(downloader, tmp_path):
    csv_root = tmp_path / "csv"
    _build_csv_volume(csv_root)

    paths = downloader.download(
        "trade_data/tick", "20240102", "20240102", options={"csv_root": str(csv_root)}
    )
    written = {p.relative_to(downloader.writer.root) for p in paths}
    assert {
        Path("2024") / "0102" / "stock.parquet",
        Path("2024") / "0102" / "tradable.parquet",
        Path("2024") / "0102" / "index.parquet",
    } <= written

    with pytest.raises(ValueError, match="csv_root"):
        downloader.download("trade_data/tick", "20240102", "20240102")


REAL_VOLUME = Path("E:/ProgramData")


@pytest.mark.skipif(
    not (REAL_VOLUME / "2024" / "0102" / "stock.parquet").is_file(),
    reason="USB data volume not mounted",
)
def test_tick_stock_schema_isomorphic_with_real_volume(tmp_path):
    """导入的 stock.parquet 必须与真实卷文件的结构一致。"""
    csv_root = tmp_path / "csv"
    root = tmp_path / "ProgramData"
    _build_csv_volume(csv_root)

    from data.downloader.tick_import import import_date_range

    import_date_range(csv_root, root, start="20240102", end="20240102")

    produced = pl.read_parquet_schema(root / "2024" / "0102" / "stock.parquet")
    real = pl.read_parquet_schema(REAL_VOLUME / "2024" / "0102" / "stock.parquet")
    assert list(produced.items()) == list(real.items())


# ---------------------------------------------------------------------------
# 旧版卷迁移
# ---------------------------------------------------------------------------


def _build_legacy_volume(root: Path) -> None:
    """微型整合前卷：全局/年度 parquet + 逐笔日。"""
    def write(frame: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)

    write(
        pd.DataFrame({"cal_date": ["20240101", "20240102"], "is_open": ["0", "1"]}),
        root / "trade_date.parquet",
    )
    write(
        pd.DataFrame({"ts_code": [INSTRUMENT], "name": ["平安银行"]}),
        root / "stock_basic.parquet",
    )
    kline = pd.DataFrame(
        {
            "ts_code": [INSTRUMENT, INSTRUMENT],
            "trade_date": ["20240102", "20240103"],
            "open": [9.39, 9.21],
            "high": [9.42, 9.4],
            "low": [9.21, 9.0],
            "close": [9.21, 9.1],
            "pre_close": [9.39, 9.21],
            "pct_chg": [-1.9, -1.2],
            "vol": [11583.66, 9000.0],
            "amount": [107574.22, 90000.0],
            "adj_factor": [116.7, 116.7],
            "name": ["平安银行", "平安银行"],
            "data_type": ["stock", "stock"],
            "timeframe": ["1d", "1d"],
            "change": [None, None],
        }
    )
    write(kline, root / "2024" / "kline.parquet")
    write(
        pd.DataFrame({"ts_code": [INSTRUMENT], "trade_date": ["20240102"], "pe_ttm": [3.68]}),
        root / "2024" / "daily_basic.parquet",
    )
    # 逐笔日保留其目录；index_member 迁移进年度库
    write(
        pd.DataFrame({"index_code": ["399006.SZ"], "con_code": [INSTRUMENT],
                      "trade_date": ["20240102"], "weight": [0.85]}),
        root / "2024" / "0102" / "index_member.parquet",
    )
    write(
        pd.DataFrame({"time": ["09:30:00"], "pr": [9.3], "vol": [100], "total_vol": [100],
                      "amount": [93000], "bs": ["B"], "code": [INSTRUMENT], "flag": [1]}),
        root / "2024" / "0102" / "stock.parquet",
    )
    # 无每日目录的年份：迁移后 K线仅存于数据库
    write(
        pd.DataFrame(
            {
                "ts_code": ["600000.SH"],
                "trade_date": ["20230630"],
                "open": [8.0], "high": [8.4], "low": [7.9], "close": [8.2],
                "pre_close": [8.0], "pct_chg": [2.5], "vol": [8000.0],
                "amount": [80000.0], "adj_factor": [100.0], "name": ["浦发银行"],
                "data_type": ["stock"], "timeframe": ["1d"], "change": [None],
            }
        ),
        root / "2023" / "kline.parquet",
    )


def test_migration_converts_validates_and_deletes(tmp_path):
    root = tmp_path / "ProgramData"
    _build_legacy_volume(root)

    # 试运行：转换 + 校验，保留所有旧版 parquet
    report = migrate_volume(root, apply=False)
    assert report.ok(), report.failures
    assert (root / "reference.duckdb").is_file()
    assert (root / "2024" / "year.duckdb").is_file()
    assert (root / "2024" / "kline.parquet").is_file()  # 试运行时保持不变
    assert (root / "2024" / "0102" / "stock.parquet").is_file()  # 逐笔卷不变

    trade_date = read_duckdb_table(root / REFERENCE_DB, "trade_date")
    assert list(trade_date["cal_date"]) == ["20240101", "20240102"]

    kline = read_duckdb_table(root / "2024" / "year.duckdb", "kline")
    base = kline[kline["timeframe"] == "1d"]
    assert len(base) == 2  # 两个交易日都从年度文件打包而来
    assert set(kline["timeframe"]) == {"1d", "5d", "10d"}
    # 逐笔日获得其每日 kline 镜像
    assert (root / "2024" / "0102" / "kline.parquet").is_file()

    member = read_duckdb_table(root / "2024" / "year.duckdb", "index_member")
    assert len(member) == 1 and member["trade_date"][0] == "20240102"

    kline_2023 = read_duckdb_table(root / "2023" / "year.duckdb", "kline")
    assert len(kline_2023[kline_2023["timeframe"] == "1d"]) == 1

    # apply：删除旧版 parquet，逐笔卷与数据库保留
    report = migrate_volume(root, apply=True)
    assert report.ok(), report.failures
    assert not (root / "trade_date.parquet").exists()
    assert not (root / "stock_basic.parquet").exists()
    assert not (root / "2024" / "kline.parquet").exists()
    assert not (root / "2024" / "daily_basic.parquet").exists()
    assert not (root / "2024" / "0102" / "index_member.parquet").exists()
    assert not (root / "2023" / "kline.parquet").exists()
    # 逐笔 parquet 保留
    assert (root / "2024" / "0102" / "stock.parquet").is_file()
    # 每日目录恰好保留逐笔卷 + 每日 kline 镜像
    assert sorted(p.name for p in (root / "2024" / "0102").iterdir()) == [
        "kline.parquet", "stock.parquet",
    ]

    # 幂等：重新迁移找不到任何待转换内容
    rerun = migrate_volume(root, apply=True)
    assert rerun.ok(), rerun.failures
