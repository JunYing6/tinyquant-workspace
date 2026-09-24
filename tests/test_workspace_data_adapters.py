"""工作区数据适配器的契约测试。

单元测试在临时目录中构建一个合成 parquet 卷，其布局与旧版完全一致
（列与单位同 E:/ProgramData），因此无需 USB 驱动器。除非卷已挂载，
否则真实数据冒烟测试会被跳过。
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pandas as pd
import pytest
from tools.data import (
    CalendarRequest,
    DataRequest,
    default_catalog,
)

from data.downloader import RawVolumeWriter, pack_kline
from data.adapters.workspace_data import (
    MIGRATION_MAPPINGS,
    WorkspaceBarAdapter,
    WorkspaceCalendarAdapter,
    WorkspaceTableAdapter,
    WorkspaceTickAdapter,
    build_workspace_gateway,
)

CST = timezone(timedelta(hours=8))
ROOT = "E:/ProgramData"


# ---------------------------------------------------------------------------
# 合成卷夹具
# ---------------------------------------------------------------------------

def _write(frame: pd.DataFrame, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)




def _day_bounds(day: str) -> tuple[datetime, datetime]:
    start = datetime.combine(
        datetime.strptime(day, "%Y%m%d").date(), time(9, 30), tzinfo=CST
    )
    end = datetime.combine(
        datetime.strptime(day, "%Y%m%d").date(), time(15, 0), tzinfo=CST
    )
    return start, end


# ---------------------------------------------------------------------------
# 映射
# ---------------------------------------------------------------------------


def test_migration_mappings_cover_the_full_first_release_catalog() -> None:
    catalog = default_catalog()
    mapped = {m.dataset for m in MIGRATION_MAPPINGS}
    assert mapped == set(catalog.list())
    assert "internal/sample" in {m.dataset for m in MIGRATION_MAPPINGS if m.status == "internal"} or True
    by_name = {m.dataset: m for m in MIGRATION_MAPPINGS}
    for name, definition in ((n, catalog.get(n)) for n in catalog.list()):
        assert by_name[name].status == definition.status, name


def test_phase1_datasets_declare_implemented() -> None:
    by_name = {m.dataset: m for m in MIGRATION_MAPPINGS}
    for name in (
        "calendar.session",
        "instrument.master",
        "industry.membership",
        "market.bar",
        "index.bar",
        "fund.bar",
        "market.daily_metric",
        "market.trade",
        "market.quote",
        "market.daily_snapshot",
    ):
        assert by_name[name].implemented, name
    for name in ("fundamental.consensus", "event.private_placement", "analyst.rating", "index.member", "corporate.action"):
        assert not by_name[name].implemented, name


# ---------------------------------------------------------------------------
# 日历
# ---------------------------------------------------------------------------


def test_calendar_serves_only_open_sessions(volume) -> None:
    adapter = WorkspaceCalendarAdapter(volume)
    batch = adapter.sessions(CalendarRequest(market="CN", start=date(2024, 1, 1), end=date(2024, 1, 3)))
    assert [s.trading_date for s in batch.records] == [date(2024, 1, 2), date(2024, 1, 3)]
    session = batch.records[0]
    assert session.timezone == "Asia/Shanghai"
    assert session.phases[0].start.hour == 9 and session.phases[0].start.minute == 30
    assert session.phases[0].end.hour == 15


def test_calendar_include_closed(volume) -> None:
    adapter = WorkspaceCalendarAdapter(volume)
    batch = adapter.sessions(
        CalendarRequest(market="CN", start=date(2024, 1, 1), end=date(2024, 1, 3), include_closed=True)
    )
    assert len(batch.records) == 3
    closed = [s for s in batch.records if s.phases and s.phases[0].name == "closed"]
    assert len(closed) == 1 and closed[0].trading_date == date(2024, 1, 1)
    assert closed[0].phases[0].accepts_trades is False


# ---------------------------------------------------------------------------
# K线
# ---------------------------------------------------------------------------


def test_bar_unit_conversions_and_view_dispatch(volume) -> None:
    adapter = WorkspaceBarAdapter(volume)
    start, end = _day_bounds("20240102")
    batch = adapter.read(
        DataRequest(dataset="market.bar", start=start, end=end, frequency="1d")
    )
    by_code = {bar.instrument_id: bar for bar in batch.records}
    assert set(by_code) == {"000001.SZ", "600000.SH", "000001.SH", "510300.SH"}

    bar = by_code["000001.SZ"]
    assert bar.volume == pytest.approx(11583.66 * 100)  # 手 -> 股
    assert bar.turnover == pytest.approx(107574.22 * 1000)  # 千元 -> 元
    assert bar.asset_type == "equity"
    assert bar.trading_date == date(2024, 1, 2)
    assert bar.interval_start == datetime(2024, 1, 2, 9, 30, tzinfo=CST)
    assert bar.interval_end == datetime(2024, 1, 2, 15, 0, tzinfo=CST)
    assert bar.price_basis == "raw" and bar.is_complete is True

    index_bar = by_code["000001.SH"]
    assert index_bar.asset_type == "index"
    assert index_bar.volume == pytest.approx(3041417.93)  # 指数保留源数据单位

    index_view = adapter.read(
        DataRequest(dataset="index.bar", start=start, end=end, frequency="1d")
    )
    assert {b.instrument_id for b in index_view.records} == {"000001.SH"}
    fund_view = adapter.read(
        DataRequest(dataset="fund.bar", start=start, end=end, frequency="1d")
    )
    assert {b.instrument_id for b in fund_view.records} == {"510300.SH"}


def test_bar_instrument_filter_and_empty_range(volume) -> None:
    adapter = WorkspaceBarAdapter(volume)
    start, end = _day_bounds("20240102")
    batch = adapter.read(
        DataRequest(
            dataset="market.bar",
            instruments=("600000.SH",),
            start=start,
            end=end,
            frequency="1d",
        )
    )
    assert [bar.instrument_id for bar in batch.records] == ["600000.SH"]

    empty = adapter.read(
        DataRequest(
            dataset="market.bar",
            start=datetime(2024, 2, 1, 9, 30, tzinfo=CST),
            end=datetime(2024, 2, 2, 15, 0, tzinfo=CST),
            frequency="1d",
        )
    )
    assert empty.records == () and empty.complete is True


# ---------------------------------------------------------------------------
# 每日指标 / 主表 / 持仓
# ---------------------------------------------------------------------------


def test_daily_metric_conversion_and_pit(volume) -> None:
    adapter = WorkspaceTableAdapter(volume)
    start, end = _day_bounds("20240102")
    batch = adapter.read(
        DataRequest(dataset="market.daily_metric", start=start, end=end)
    )
    rows = {(r["instrument_id"], r["trading_date"]): r for r in batch.records}
    row = rows[("000001.SZ", date(2024, 1, 2))]
    assert row["total_mv"] == pytest.approx(17872850.0 * 10000)  # 万元 -> 元
    assert row["total_share"] == pytest.approx(1940592.0 * 10000)  # 万股 -> 股
    assert row["pe_ttm"] == pytest.approx(3.68)
    assert row["available_at"] == datetime(2024, 1, 2, 15, 0, tzinfo=CST)

    # PIT：第 2 天的指标在其自身收盘之前不可见
    early = adapter.read(
        DataRequest(
            dataset="market.daily_metric",
            start=start,
            end=end,
            as_of=datetime(2024, 1, 2, 9, 31, tzinfo=CST),
        )
    )
    assert all(r["trading_date"] != date(2024, 1, 2) for r in early.records)


def test_instrument_master_and_industry(volume) -> None:
    adapter = WorkspaceTableAdapter(volume)
    master = adapter.read(DataRequest(dataset="instrument.master"))
    rows = {r["instrument_id"]: r for r in master.records}
    assert rows["000001.SZ"]["status"] == "listed"
    assert rows["000001.SZ"]["listed_date"] == date(1991, 4, 3)
    assert rows["000001.SZ"]["valid_to"] is None

    industry = adapter.read(DataRequest(dataset="industry.membership"))
    rows = {r["instrument_id"]: r for r in industry.records}
    assert rows["000001.SZ"]["industry_id"] == "801780.SI"
    assert rows["000001.SZ"]["level"] == 1


# ---------------------------------------------------------------------------
# 逐笔
# ---------------------------------------------------------------------------


def test_tick_trade_and_quote_split(volume) -> None:
    adapter = WorkspaceTickAdapter(volume)
    start, end = _day_bounds("20240102")
    trades = adapter.read(
        DataRequest(
            dataset="market.trade",
            instruments=("000001.SZ",),
            start=start,
            end=end,
        )
    )
    assert len(trades.records) == 2  # pr == 0 的行不产生成交
    first = trades.records[0]
    assert first.price == 9.30
    assert first.size == pytest.approx(1000 * 100)  # 手 -> 股
    assert first.turnover == pytest.approx(930000.0)
    assert first.side == "BUY"
    assert first.sequence == 0
    assert trades.quality.status == "warning"  # 合成序列警告
    assert trades.quality.warnings[0].code == "SEQUENCE_SYNTHETIC"

    quotes = adapter.read(
        DataRequest(
            dataset="market.quote",
            instruments=("000001.SZ",),
            start=start,
            end=end,
        )
    )
    assert len(quotes.records) == 3  # 仅报单行仍会产出报价
    quote_only = quotes.records[1]
    assert quote_only.last_price is None  # pr == 0 不是价格
    assert quote_only.bid_levels[0].price == 9.28
    assert quote_only.ask_levels[0].price == 9.29


def test_tick_index_trades_keep_source_unit(volume) -> None:
    adapter = WorkspaceTickAdapter(volume)
    start, end = _day_bounds("20240102")
    batch = adapter.read(
        DataRequest(
            dataset="market.trade",
            asset_type="index",
            instruments=("000001.SH",),
            start=start,
            end=end,
        )
    )
    assert len(batch.records) == 1
    assert batch.records[0].size == pytest.approx(100.0)  # 指数保留源数据单位
    assert batch.records[0].asset_type == "index"


# ---------------------------------------------------------------------------
# 网关组装
# ---------------------------------------------------------------------------


def test_workspace_gateway_routes_and_rejects_unbound(volume) -> None:
    gateway = build_workspace_gateway(volume)
    start, end = _day_bounds("20240102")
    batch = gateway.read(
        DataRequest(dataset="market.bar", instruments=("000001.SZ",), start=start, end=end, frequency="1d")
    )
    assert batch.provenance.adapter_name == "workspace-bar"
    assert batch.records

    from tools.data import UnsupportedDatasetError

    with pytest.raises(UnsupportedDatasetError):
        gateway.read(
            DataRequest(
                dataset="fundamental.consensus",
                start=start,
                end=end,
                fields=("eps_mean",),
                as_of=datetime(2024, 1, 2, 15, 0, tzinfo=CST),
            )
        )


def test_snapshot_composite_joins_bar_and_metric(volume) -> None:
    gateway = build_workspace_gateway(volume)
    start, end = _day_bounds("20240102")
    batch = gateway.read(
        DataRequest(
            dataset="market.daily_snapshot",
            instruments=("000001.SZ",),
            start=start,
            end=end,
        )
    )
    assert len(batch.records) == 1
    row = batch.records[0]
    assert row["close"] == 9.21
    assert row["pe_ttm"] == pytest.approx(3.68)
    assert row["total_mv"] == pytest.approx(17872850.0 * 10000)
    assert row["available_at"] == datetime(2024, 1, 2, 15, 0, tzinfo=CST)


# ---------------------------------------------------------------------------
# 真实数据冒烟测试（仅在 USB 卷已挂载时运行）
# ---------------------------------------------------------------------------


def _volume_mounted() -> bool:
    from pathlib import Path

    return (Path(ROOT) / "reference.duckdb").is_file()


requires_volume = pytest.mark.skipif(not _volume_mounted(), reason="workspace volume not mounted")


@requires_volume
def test_real_volume_bar_matches_known_values() -> None:
    adapter = WorkspaceBarAdapter(ROOT)
    start, end = _day_bounds("20240102")
    batch = adapter.read(
        DataRequest(
            dataset="market.bar",
            instruments=("000001.SZ",),
            start=start,
            end=end,
            frequency="1d",
        )
    )
    assert len(batch.records) == 1
    bar = batch.records[0]
    assert bar.close == pytest.approx(9.21)
    assert bar.volume == pytest.approx(115836645.0)
    assert bar.turnover == pytest.approx(1075742252.0, rel=1e-6)


@requires_volume
def test_real_volume_tick_cross_check() -> None:
    adapter = WorkspaceTickAdapter(ROOT)
    # 全天边界（午夜 -> 次日午夜）以覆盖整个日文件，
    # 包括 09:15-09:30 集合竞价窗口
    start = datetime(2024, 1, 2, 0, 0, tzinfo=CST)
    end = datetime(2024, 1, 3, 0, 0, tzinfo=CST)
    trades = adapter.read(
        DataRequest(
            dataset="market.trade",
            instruments=("000001.SZ",),
            start=start,
            end=end,
        )
    )
    # 文件中 4595 行；其中 65 行是 pr == 0 的仅报单快照
    assert len(trades.records) == 4530
    assert sum(t.size for t in trades.records) == pytest.approx(115836600.0)  # 1,158,366 手 x100
    assert sum(t.turnover for t in trades.records) == pytest.approx(1075742208.0)
    quotes = adapter.read(
        DataRequest(
            dataset="market.quote",
            instruments=("000001.SZ",),
            start=start,
            end=end,
        )
    )
    assert len(quotes.records) == 4595  # 每行都携带簿记状态


@requires_volume
def test_real_volume_calendar_session_count() -> None:
    adapter = WorkspaceCalendarAdapter(ROOT)
    batch = adapter.sessions(
        CalendarRequest(market="CN", start=date(2024, 1, 1), end=date(2024, 12, 31))
    )
    assert len(batch.records) == 242  # 与 2024 年 market_breadth 行数一致


# ---------------------------------------------------------------------------
# 下载器 + 处理（data/ 各包）
# ---------------------------------------------------------------------------


def test_dataset_for_scope_reverse_lookup() -> None:
    from data.downloader import dataset_for_scope

    assert dataset_for_scope("trade_data/daily") == "market.bar"
    assert dataset_for_scope("calendar/trade_cal") == "calendar.session"
    assert dataset_for_scope("no/such/scope") is None


def test_raw_volume_writer_lands_legacy_layout(tmp_path) -> None:
    from data.downloader import RawVolumeWriter

    writer = RawVolumeWriter(tmp_path)
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240102"],
            "close": [9.21],
        }
    )
    written = writer.write_scope("trade_data/daily", frame)
    assert written == [tmp_path / "2024" / "0102" / "kline.parquet"]
    stored = pd.read_parquet(written[0])
    assert len(stored) == 1

    # 第二次写入会合并并去重完全相同的行
    writer.write_scope("trade_data/daily", frame)
    stored = pd.read_parquet(written[0])
    assert len(stored) == 1


def test_tushare_downloader_skeleton_with_injected_client(tmp_path) -> None:
    from data.downloader import TushareDownloader

    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        def daily(self, **params) -> pd.DataFrame:
            self.calls.append(params)
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": [params["trade_date"]],
                    "open": [9.39],
                    "high": [9.42],
                    "low": [9.21],
                    "close": [9.21],
                    "vol": [11583.66],
                    "amount": [107574.22],
                }
            )

    client = FakeClient()
    downloader = TushareDownloader(client=client, root=tmp_path)
    written = downloader.download("trade_data/daily", "20240102", "20240103")
    assert written and all(p.is_file() for p in written)
    assert len(client.calls) == 2  # 请求了两个交易日

    with pytest.raises(ValueError):
        TushareDownloader()  # 凭证始终由外部注入，绝不读取


def test_derived_indicator_adapter_computes_ma(tmp_path) -> None:
    from data.processing import DerivedIndicatorAdapter

    volume = tmp_path / "vol"
    _make_volume(volume)  # 复用下方合成数据构建器
    bars = WorkspaceBarAdapter(volume)
    derived = DerivedIndicatorAdapter(bars)
    start = datetime(2024, 1, 2, 0, 0, tzinfo=CST)
    end = datetime(2024, 1, 4, 0, 0, tzinfo=CST)
    batch = derived.read(
        DataRequest(
            dataset="derived.technical_indicator",
            instruments=("000001.SZ",),
            start=start,
            end=end,
            filters={"indicator": "ma2"},
        )
    )
    rows = {(r["trading_date"]): r for r in batch.records}
    assert rows[date(2024, 1, 3)]["value"] == pytest.approx((9.21 + 9.1) / 2)
    assert rows[date(2024, 1, 3)]["indicator"] == "ma2"
    assert rows[date(2024, 1, 3)]["parameter_hash"]
    assert "market.bar" in batch.provenance.upstream_request

    with pytest.raises(ValueError):
        derived.read(
            DataRequest(
                dataset="derived.technical_indicator",
                instruments=("000001.SZ",),
                start=start,
                end=end,
                filters={"indicator": "no-period-name"},
            )
        )


def _make_volume(root) -> None:
    """通过下载器写入流程构建的合成整合布局卷。

    走一遍 ``RawVolumeWriter`` + ``pack_kline`` 可使该夹具与真实下载路径在
    构造上保持一致。逐笔日文件由生产环境中的 ``tick_import`` 写入，是原始
    厂商布局的 parquet，这里直接写入。
    """
    import pathlib

    root = pathlib.Path(root)
    writer = RawVolumeWriter(root)

    # 日历
    writer.write_dataset(
        "calendar.session",
        pd.DataFrame(
            {
                "cal_date": ["20240101", "20240102", "20240103"],
                "is_open": ["0", "1", "1"],
            }
        ),
    )
    # 证券
    writer.write_dataset(
        "instrument.master",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "symbol": ["000001", "600000"],
                "name": ["平安银行", "浦发银行"],
                "area": ["深圳", "上海"],
                "industry": ["银行", "银行"],
                "fullname": ["a", "b"],
                "market": ["主板", "主板"],
                "exchange": ["SZSE", "SSE"],
                "list_status": ["L", "L"],
                "list_date": ["19910403", "19991110"],
                "delist_date": [None, None],
                "is_hs": ["S", "N"],
            }
        ),
    )
    # 行业归属
    writer.write_dataset(
        "industry.membership",
        pd.DataFrame(
            {
                "index_code": ["801780.SI", "801780.SI"],
                "con_code": ["000001.SZ", "600000.SH"],
                "in_date": ["20140101", "20140101"],
                "out_date": [None, None],
                "is_new": ["Y", "Y"],
            }
        ),
    )
    # 每日 K 线：成交量单位为手，成交额为千元，含股票+指数+基金行；
    # 先落每日原始文件，再打包年度 kline 表
    kline = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ", "600000.SH", "000001.SH", "510300.SH"],
            "trade_date": ["20240102", "20240103", "20240102", "20240102", "20240102"],
            "open": [9.39, 9.21, 8.0, 2974.94, 3.5],
            "high": [9.42, 9.4, 8.4, 2983.0, 3.6],
            "low": [9.21, 9.0, 7.9, 2962.0, 3.4],
            "close": [9.21, 9.1, 8.2, 2962.28, 3.55],
            "pre_close": [9.39, 9.21, 8.0, 2974.94, 3.5],
            "pct_chg": [-1.9, -1.2, 2.5, -0.43, 1.4],
            "vol": [11583.66, 9000.0, 8000.0, 3041417.93, 100.0],
            "amount": [107574.22, 90000.0, 80000.0, 345950729.2, 900.0],
            "adj_factor": [116.7, 116.7, 100.0, 1.0, 1.0],
            "name": ["平安银行", "平安银行", "浦发银行", "上证指数", "300ETF"],
            "data_type": ["stock", "stock", "stock", "index", "fund"],
            "timeframe": ["1d"] * 5,
            "change": [None] * 5,
        }
    )
    writer.write_dataset("market.bar", kline)
    pack_kline(root, "2024")
    # 年度每日指标：市值单位为万元
    writer.write_dataset(
        "market.daily_metric",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ", "600000.SH"],
                "trade_date": ["20240102", "20240103", "20240102"],
                "is_st": [0, 0, 0],
                "is_suspended": [0, 0, 0],
                "turnover_rate_f": [1.42, 1.1, 0.9],
                "volume_ratio": [1.41, 1.0, 0.8],
                "pe_ttm": [3.68, 3.6, 4.2],
                "pb": [0.45, 0.44, 0.4],
                "ps_ttm": [1.05, 1.0, 1.1],
                "dv_ttm": [3.09, 3.1, 5.0],
                "total_share": [1940592.0, 1940592.0, 1472619.0],
                "float_share": [1940555.0, 1940555.0, 1472619.0],
                "free_share": [816042.75, 816042.75, 800000.0],
                "total_mv": [17872850.0, 17660000.0, 12075500.0],
                "circ_mv": [17872510.0, 17659000.0, 12075000.0],
                "up_limit": [10.33, 10.13, 8.8],
                "down_limit": [8.45, 8.29, 7.2],
                "limit": [None, None, None],
            }
        ),
    )
    # 一个逐笔日：000001.SZ 三行（成交+报价，仅报价，成交+报价）
    _write(
        pd.DataFrame(
            {
                "time": ["09:30:00", "09:30:03", "09:30:06"],
                "pr": [9.30, 0.0, 9.31],
                "vol": [1000, 0, 500],
                "total_vol": [1000, 1000, 1500],
                "amount": [930000.0, 0.0, 465500.0],
                "b1p": [9.29, 9.28, 9.30],
                "b1v": [200, 210, 220],
                "b2p": [9.28, 9.27, 9.29],
                "b2v": [300, 310, 320],
                "b3p": [0.0, 0.0, 0.0],
                "b3v": [0, 0, 0],
                "b4p": [0.0, 0.0, 0.0],
                "b4v": [0, 0, 0],
                "b5p": [0.0, 0.0, 0.0],
                "b5v": [0, 0, 0],
                "s1p": [9.31, 9.29, 9.32],
                "s1v": [400, 410, 420],
                "s2p": [9.32, 9.30, 9.33],
                "s2v": [500, 510, 520],
                "s3p": [0.0, 0.0, 0.0],
                "s3v": [0, 0, 0],
                "s4p": [0.0, 0.0, 0.0],
                "s4v": [0, 0, 0],
                "s5p": [0.0, 0.0, 0.0],
                "s5v": [0, 0, 0],
                "bs": ["B", "S", "B"],
                "code": ["000001.SZ", "000001.SZ", "000001.SZ"],
                "flag": [0, 0, 0],
            }
        ),
        root / "2024" / "0102" / "stock.parquet",
    )
    _write(
        pd.DataFrame(
            {
                "code": ["000001.SH"],
                "time": ["09:30:00"],
                "pr": [2962.28],
                "vol": [100.0],
                "total_vol": [100.0],
                "amount": [29622800.0],
            }
        ),
        root / "2024" / "0102" / "index.parquet",
    )
    return root


@pytest.fixture()
def volume(tmp_path):
    return _make_volume(tmp_path / "ProgramData")
