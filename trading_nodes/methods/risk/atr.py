"""ATR 止损风控控制器（RiskControl）。

把 AtrStopRiskFactor 集成到风险控制流程：盘前默认允许全仓交易，
盘中逐根 K 线评估止损信号，一旦触发则禁止交易并将目标仓位降至 0。
"""

from __future__ import annotations

from collections import deque
from typing import Any

from trading_nodes.factors.risk.kline.atr_stop import AtrStopRiskFactor
from trading_nodes_base.types import KlineBar, RiskSignal
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
        return self.risk_decision

    def on_bar(self, bar: KlineBar) -> RiskDecision:
        signals = self.risk_kline_factors[0].on_bar(bar)
        if any(signal.triggered for signal in signals):
            # 触发止损：禁止交易并清仓（目标仓位 0）。
            self.risk_decision = RiskDecision(False, 0.0, True)
            self.is_trading_allowed = False
            self.target_position_ratio = 0.0
        return self.risk_decision
