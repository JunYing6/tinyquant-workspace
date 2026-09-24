"""满仓风险控制器（FullPositionRisk）。

最简单的风险控制：始终允许交易、目标仓位保持 100%，即不做任何风控干预，
始终满仓运行。常用于演示或无需风控约束的策略。
"""

from __future__ import annotations

from typing import Any

from trading_nodes_base.methods import BaseRiskControl, RiskDecision


class FullPositionRisk(BaseRiskControl):
    """始终允许交易且目标仓位 100% 的满仓风控器。"""

    def __init__(self) -> None:
        super().__init__("full-position-risk")

    def on_pre_market(self) -> RiskDecision:
        # 盘前：允许交易，目标仓位 100%。
        self.risk_decision = RiskDecision(True, 1.0, False)
        self.is_trading_allowed = True
        self.target_position_ratio = 1.0
        return self.risk_decision

    def on_daily(self) -> RiskDecision:
        # 盘中/日终保持一致：仍为满仓允许交易。
        return self.on_pre_market()


__all__ = ["FullPositionRisk"]
