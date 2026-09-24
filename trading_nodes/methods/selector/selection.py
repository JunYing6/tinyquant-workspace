"""基于因子的选股器（Selector）组合。

把"因子（打分层）"与"选股器（结果层）"组装起来：
- MomentumSelector：动量打分选股，用 MomentumSelectionFactor 排序。
- PriceFilterSelector：价格均线过滤选股，用 PriceAboveMaSelectionFactor 过滤。

注意：这两个选股器在 select_stocks() 阶段仍直接返回给定的 stock_pool，
因子层由框架在择时/打分阶段消费，此处体现"因子 + 选股器"的节点结构。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from trading_nodes_base.factors import BinarySelectionFactor, FloatSelectionFactor
from importlib import import_module

PriceAboveMaSelectionFactor = import_module("trading_nodes.factors.selector.and.price_above_ma").PriceAboveMaSelectionFactor
MomentumSelectionFactor = import_module("trading_nodes.factors.selector.float.momentum").MomentumSelectionFactor
from trading_nodes_base.methods import BaseStockPicking


def _pool_frame(stock_pool: Sequence[str]) -> pd.DataFrame:
    """把股票池转为等权 asset/weight 数据表；空池返回空表。"""
    if not stock_pool:
        return pd.DataFrame(columns=["asset", "weight"])
    weight = 1.0 / len(stock_pool)
    return pd.DataFrame({"asset": list(stock_pool), "weight": [weight] * len(stock_pool)})


class MomentumSelector(BaseStockPicking):
    """动量选股器：搭载动量打分因子。"""

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        self.stock_pool = list(dict.fromkeys(stock_pool or []))
        factor = MomentumSelectionFactor()
        super().__init__("momentum-selector", [], [], [factor])

    def select_stocks(self, date: Any) -> pd.DataFrame:
        return _pool_frame(self.stock_pool)


class PriceFilterSelector(BaseStockPicking):
    """价格过滤选股器：搭载"股价站上均线"过滤因子。"""

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        self.stock_pool = list(dict.fromkeys(stock_pool or []))
        factor = PriceAboveMaSelectionFactor()
        super().__init__("price-filter-selector", [factor], [], [])

    def select_stocks(self, date: Any) -> pd.DataFrame:
        return _pool_frame(self.stock_pool)


__all__ = ["MomentumSelectionFactor", "MomentumSelector", "PriceAboveMaSelectionFactor", "PriceFilterSelector"]
