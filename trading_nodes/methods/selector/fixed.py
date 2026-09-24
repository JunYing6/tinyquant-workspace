"""指定股票选股器（FixedStockPicking）。

"指定股票"选股：不做任何因子筛选或排名，直接把用户显式给定的股票池
按等权返回，供策略作为候选标的池使用。若未指定池，则返回空。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from trading_nodes_base.methods import BaseStockPicking


class FixedStockPicking(BaseStockPicking):
    """直接采用给定股票池、等权分配的选股器。"""

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        # 去重后保存显式股票池；None/空 -> 空池
        self.stock_pool = list(dict.fromkeys(stock_pool or []))
        super().__init__("fixed-stock-picking", [], [], [])

    def select_stocks(self, date: Any) -> pd.DataFrame:
        if not self.stock_pool:
            return pd.DataFrame(columns=["asset", "weight"])  # 空池返回空结果
        weight = 1.0 / len(self.stock_pool)  # 等权
        return pd.DataFrame({"asset": self.stock_pool, "weight": [weight] * len(self.stock_pool)})


__all__ = ["FixedStockPicking"]
