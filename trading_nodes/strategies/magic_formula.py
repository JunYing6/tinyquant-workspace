"""示例：乔尔·格林布莱特的神奇公式（Magic Formula）。

神奇公式用一个候选股票池在两个"质量/估值"因子上排序，持有排名靠前的
标的，并周期性调仓（格林布莱特建议持有 12 个月；本示例采用半年度调仓）。

因子一 —— 盈利收益率 = EBIT / 企业价值
    EBIT = operate_profit + fin_exp          (营业利润 + 财务费用)
    EV   = total_mv + st_borr + lt_borr - money_cap
                                    (总市值 + 有息负债 - 货币资金)
    越高越好。

因子二 —— 资本回报率 = EBIT / 投入资本
    用 ``roic`` 财务指标实现（EBIT / 投入资本），即格林布莱特第二因子的
    直接体现。越高越好。

综合得分：在股票池上对每个因子降序排名，两因子排名求和，取综合排名
之和最小的 ``top_n`` 只，等权持有。

数据需求：因子读取 ``fundamental.report``（利润表、资产负债表与 ``roic``
指标），以及来自 ``market.daily_metric`` 的 ``total_mv``。一阶段工作区
网关只提供行情数据；接入基本面适配器后便可在端到端跑通。因子与选股
计算是提供者无关的，并由单元测试校验。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd  # type: ignore[import-untyped]

from trading_nodes_base.factors import FloatSelectionFactor, KlineTimingFactor
from trading_nodes_base.methods import BaseStockPicking, BaseTimeSelection
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.types import SignalIntent
from trading_nodes.factors.timer.tick.intent_executor import IntentExecutorFactor
from trading_nodes.methods.risk.full_position import FullPositionRisk


def _records(payload: Any) -> list:
    records = getattr(payload, "records", None)
    rows = records if records is not None else payload
    if hasattr(rows, "to_dict"):
        return rows.to_dict("records")
    return list(rows or [])


def _as_record(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    return dict(vars(row))


class EarningsYieldFactor(FloatSelectionFactor):
    """盈利收益率因子（EBIT / 企业价值），神奇公式的第一个打分维度。"""

    def __init__(self, universe: Sequence[str]) -> None:
        super().__init__("earnings-yield")
        self.universe = list(dict.fromkeys(universe))

    def get_query_lst(self, date: Any, codes: Optional[list[str]] = None) -> list[dict]:
        self._data_clear()
        universe = codes or self.universe
        # 声明式数据需求：利润表 + 资产负债表字段 + 总市值
        return [
            {
                "dataset": "fundamental.income",
                "date": date,
                "codes": universe,
                "fields": ["operate_profit", "fin_exp"],
            },
            {
                "dataset": "fundamental.balance",
                "date": date,
                "codes": universe,
                "fields": ["st_borr", "lt_borr", "money_cap"],
            },
            {
                "dataset": "market.daily_metric",
                "date": date,
                "codes": universe,
                "fields": ["total_mv"],
            },
        ]

    def _calculate_internal(self, data_cache: Dict[str, Any]) -> pd.Series:
        merged: Dict[str, Dict[str, float]] = {}
        for payload in data_cache.values():
            for row in _records(payload):
                rec = _as_record(row)
                code = rec.get("ts_code") or rec.get("code") or rec.get("instrument_id")
                if not code:
                    continue
                bucket = merged.setdefault(code, {})
                for key in ("operate_profit", "fin_exp", "st_borr", "lt_borr", "money_cap", "total_mv"):
                    if rec.get(key) is not None:
                        bucket[key] = float(rec[key])
        values: Dict[str, float] = {}
        for code, fields in merged.items():
            operate_profit = fields.get("operate_profit")
            total_mv = fields.get("total_mv")
            if operate_profit is None or not total_mv or total_mv <= 0:
                continue  # 关键数据缺失则跳过
            ebit = operate_profit + (fields.get("fin_exp") or 0.0)  # EBIT = 营业利润 + 财务费用
            if ebit <= 0:
                continue
            debt = (fields.get("st_borr") or 0.0) + (fields.get("lt_borr") or 0.0)
            cash = fields.get("money_cap") or 0.0
            enterprise_value = total_mv + debt - cash  # EV = 总市值 + 有息负债 - 货币资金
            if enterprise_value <= 0:
                continue
            values[code] = ebit / enterprise_value  # 盈利收益率越高越好
        return pd.Series(values, dtype=float)


class ReturnOnCapitalFactor(FloatSelectionFactor):
    """资本回报率因子（用 roic 指标），神奇公式的第二打分维度。"""

    def __init__(self, universe: Sequence[str]) -> None:
        super().__init__("return-on-capital")
        self.universe = list(dict.fromkeys(universe))

    def get_query_lst(self, date: Any, codes: Optional[list[str]] = None) -> list[dict]:
        self._data_clear()
        # 声明式数据需求：roic 财务指标
        return [
            {
                "dataset": "fundamental.indicator",
                "date": date,
                "codes": codes or self.universe,
                "fields": ["roic"],
            }
        ]

    def _calculate_internal(self, data_cache: Dict[str, Any]) -> pd.Series:
        values: Dict[str, float] = {}
        for payload in data_cache.values():
            for row in _records(payload):
                rec = _as_record(row)
                code = rec.get("ts_code") or rec.get("code") or rec.get("instrument_id")
                roic = rec.get("roic")
                if code and roic is not None:
                    try:
                        value = float(roic)
                    except (TypeError, ValueError):
                        continue
                    if value > 0:
                        values[code] = value  # 资本回报率越高越好
        return pd.Series(values, dtype=float)


class MagicFormulaPicking(BaseStockPicking):
    """按两个因子对股票池排序，选取综合排名靠前的标的（备选池）。"""

    def __init__(self, universe: Sequence[str], top_n: int = 30) -> None:
        self.top_n = int(top_n)
        earnings = EarningsYieldFactor(universe)
        roic = ReturnOnCapitalFactor(universe)
        super().__init__("magic-formula-picking", [], [], [earnings, roic])
        self.earnings_factor = earnings
        self.roic_factor = roic

    @staticmethod
    def _empty() -> pd.DataFrame:
        return pd.DataFrame(columns=["asset", "weight"])

    @staticmethod
    def _select_from_scores(
        earnings_yield: pd.Series, roic: pd.Series, top_n: int
    ) -> pd.DataFrame:
        frame = pd.concat([earnings_yield, roic], axis=1, join="inner")
        if frame.empty:
            return MagicFormulaPicking._empty()
        frame.columns = ["earnings_yield", "roic"]
        # 只保留两因子均为正的盈利标的
        profitable = (frame["earnings_yield"] > 0) & (frame["roic"] > 0)
        frame = frame[profitable].dropna()
        if frame.empty:
            return MagicFormulaPicking._empty()
        # 各因子降序排名后求和，取排名之和最小（综合最优）的 top_n 只等权持有
        rank_yield = frame["earnings_yield"].rank(ascending=False)
        rank_roic = frame["roic"].rank(ascending=False)
        combined = (rank_yield + rank_roic).sort_values()
        top = combined.head(top_n).index
        weight = 1.0 / len(top)
        return pd.DataFrame({"asset": list(top), "weight": [weight] * len(top)})

    def select_stocks(self, date: Any) -> pd.DataFrame:
        return self._select_from_scores(
            self.earnings_factor.calculate(date),
            self.roic_factor.calculate(date),
            self.top_n,
        )


class MagicFormulaRebalanceFactor(KlineTimingFactor):
    """半年度调仓择时因子：卖出退出名单、买入新入选名单。"""

    frequency = "1d"
    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, rebalance_months: Sequence[int] = (1, 7)) -> None:
        super().__init__("magic-formula-rebalance")
        self.rebalance_months = tuple(rebalance_months)
        self.timer: Optional[BaseTimeSelection] = None
        self._last_rebalance: Optional[tuple] = None
        self._emitted: set[str] = set()

    def get_query_lst(self, date: Any, codes: Optional[list[str]] = None) -> list[dict]:
        self._data_clear()
        return []

    def on_bar(self, bar: Any) -> List[SignalIntent]:
        end_time = getattr(bar, "end_time", None)
        if isinstance(end_time, datetime) and end_time.month not in self.rebalance_months:
            return []  # 非调仓月份不动作
        if isinstance(end_time, datetime):
            period_key = (end_time.year, end_time.month)
        else:
            return []
        if period_key != self._last_rebalance:
            # 进入新的调仓周期：刷新上次周期记录与已发信号集合
            self._last_rebalance = period_key
            self._emitted.clear()
        code = getattr(bar, "code", None)
        if not code or code in self._emitted:
            return []  # 每个周期每只股票只发一次信号
        target = set(self.timer.selector_pool) if self.timer else set()  # 应持有名单
        held = set(self.timer.position_pool) if self.timer else set()    # 当前持仓名单
        intents: List[SignalIntent] = []
        if code in target and code not in held:
            # 新入选 -> 买入
            size = 1.0 / len(target) if target else 1.0
            intents.append(SignalIntent(
                code, "BUY", end_time, "magic-formula-buy", {"sizing_intent": size}
            ))
            self._emitted.add(code)
        elif code in held and code not in target:
            # 被剔除 -> 卖出
            intents.append(SignalIntent(
                code, "SELL", end_time, "magic-formula-sell", {"sizing_intent": 1.0}
            ))
            self._emitted.add(code)
        return intents


class MagicFormulaStrategy(BaseStrategy):
    """持有神奇公式选出的标的，半年度再平衡。"""

    supports_fast_backtest = True

    def __init__(
        self,
        universe: Optional[Sequence[str]] = None,
        *,
        top_n: int = 30,
        rebalance_months: Sequence[int] = (1, 7),
    ) -> None:
        universe = list(universe or ["000001.SZ", "600000.SH"])
        selector = MagicFormulaPicking(universe, top_n=top_n)
        rebalance = MagicFormulaRebalanceFactor(rebalance_months)
        timer = BaseTimeSelection(
            "magic-formula-timer",
            kline_factors=[rebalance],
            tick_factors=[IntentExecutorFactor()],
        )
        rebalance.timer = timer
        super().__init__(
            strategy_name="magic-formula",
            selector=selector,
            timer=timer,
            risk_ctrl=FullPositionRisk(),
        )


__all__ = [
    "EarningsYieldFactor",
    "MagicFormulaPicking",
    "MagicFormulaRebalanceFactor",
    "MagicFormulaStrategy",
    "ReturnOnCapitalFactor",
]
