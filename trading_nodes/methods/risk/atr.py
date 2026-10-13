"""ATR 止损风控控制器（RiskControl）。

把 AtrStopRiskFactor 集成到风险控制流程：盘前默认允许全仓交易，并
在盘前用声明式窗口计算 ATR 止损信号，一旦触发则禁止交易并将目标仓位降至 0。
"""

from __future__ import annotations

from typing import Any

from trading_nodes.factors.risk.kline.atr_stop import AtrStopRiskFactor
from trading_nodes_base.methods import BaseRiskControl, RiskDecision


class AtrRiskControl(BaseRiskControl):
    """基于 ATR 止损的风险控制器。"""

    def __init__(self, window: int = 14, multiple: float = 2.0) -> None:
        super().__init__("atr-risk", risk_kline_factors=[AtrStopRiskFactor(window, multiple)])

    def on_pre_market(self) -> RiskDecision:
        # 盘前默认：允许交易，目标仓位 100%。
        self.risk_decision = RiskDecision(True, 1.0, False)
        self.is_trading_allowed = True
        self.target_position_ratio = 1.0
        # 分层结果 + 非日内风控因子（声明式窗口）盘前计算
        super().on_pre_market()
        if self.target_position_ratio <= 0:
            # 触发止损：禁止交易并强制清仓
            self.is_trading_allowed = False
            self.target_position_ratio = 0.0
            self.risk_decision = RiskDecision(False, 0.0, True)
        return self.risk_decision
