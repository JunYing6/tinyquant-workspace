from __future__ import annotations

from typing import Any

from config import secrets, settings
from engines.realtime import RealTimeTradeEngine

try:
    from data.realtime.jvquant import JvQuantQuoteClient
except ImportError:  # pragma: no cover - optional dependency
    JvQuantQuoteClient = None
from data.realtime.mock import MockQuoteClient


class PaperTradeExecutor:
    def __init__(self, config: dict | None = None) -> None:
        self._config = config or {}
        self._cash = float(self._config.get("initial_cash", 1_000_000))
        self._positions: dict[str, dict[str, float]] = {}

    def connect(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def buy(self, symbol: str, volume: int, price: float = 0.0, **kwargs: Any) -> Any:
        volume = int(volume)
        if volume <= 0 or price <= 0:
            return {"success": False}
        self._cash -= price * volume
        position = self._positions.setdefault(symbol, {"volume": 0.0, "cost_price": 0.0})
        total_cost = position["cost_price"] * position["volume"] + price * volume
        position["volume"] += volume
        position["cost_price"] = total_cost / position["volume"]
        return {"success": True}

    def sell(self, symbol: str, volume: int, price: float = 0.0, **kwargs: Any) -> Any:
        volume = int(volume)
        position = self._positions.get(symbol)
        if position is None or volume <= 0:
            return {"success": False}
        if volume > position["volume"]:
            return {"success": False}
        if price > 0:
            self._cash += price * volume
        position["volume"] -= volume
        if position["volume"] <= 0:
            self._positions.pop(symbol, None)
        return {"success": True}

    def get_positions(self) -> list[dict[str, Any]]:
        return [
            {"symbol": symbol, "volume": int(position["volume"]), "cost_price": position["cost_price"]}
            for symbol, position in self._positions.items()
            if position["volume"] > 0
        ]

    def get_account(self) -> dict[str, float]:
        market_value = sum(
            position["cost_price"] * position["volume"] for position in self._positions.values()
        )
        return {"available_cash": self._cash, "total_assets": self._cash + market_value}


class GMTradeExecutor:
    def __init__(self, config: dict | None = None) -> None:
        self._config = config or {}
        self._connected = False

    def _gm(self):
        try:
            from gm.api import get_cash, get_positions, order_volume  # noqa: F401
        except ImportError as error:
            raise RuntimeError("gm not installed; pip install gm") from error
        return order_volume, get_positions, get_cash

    def connect(self) -> None:
        try:
            from gm.api import set_endpoint, set_token
        except ImportError as error:
            raise RuntimeError("gm not installed; pip install gm") from error
        token = self._config.get("token") or getattr(secrets, "GM_TOKEN", "")
        if not token:
            raise ValueError("missing gm token")
        set_token(token)
        mode = self._config.get("mode", "live")
        endpoint = self._config.get("endpoint") or (
            "sim-trading.myquant.cn:8001" if mode == "sim" else "trading.myquant.cn:8001"
        )
        set_endpoint(endpoint)
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def buy(self, symbol: str, volume: int, price: float = 0.0, **kwargs: Any) -> Any:
        if not self._connected:
            self.connect()
        order_volume, _, _ = self._gm()
        order_type = kwargs.get("order_type")
        limit = order_type is not None and getattr(order_type, "value", None) == "limit"
        order_volume(symbol=symbol, volume=volume, side=1, order_type=2 if limit else 1, position_effect=1, price=price if limit else 0)
        return {"success": True}

    def sell(self, symbol: str, volume: int, price: float = 0.0, **kwargs: Any) -> Any:
        if not self._connected:
            self.connect()
        order_volume, _, _ = self._gm()
        order_type = kwargs.get("order_type")
        limit = order_type is not None and getattr(order_type, "value", None) == "limit"
        order_volume(symbol=symbol, volume=volume, side=2, order_type=2 if limit else 1, position_effect=2, price=price if limit else 0)
        return {"success": True}

    def get_positions(self) -> list[Any]:
        _, get_positions, _ = self._gm()
        return list(get_positions())

    def get_account(self) -> Any:
        _, _, get_cash = self._gm()
        return get_cash()


EXECUTORS = {
    "paper": PaperTradeExecutor,
    "gm": GMTradeExecutor,
}

QUOTE_SOURCES: dict[str, Any] = {"mock": MockQuoteClient}
if JvQuantQuoteClient is not None:
    QUOTE_SOURCES["jvquant"] = JvQuantQuoteClient


def build_executor(route: str = "paper", config: dict | None = None) -> Any:
    cls = EXECUTORS.get(route)
    if cls is None:
        raise ValueError(f"unknown executor route: {route!r}")
    return cls(config or {})


def build_quote_client(route: str = "mock", token: str = "") -> Any:
    cls = QUOTE_SOURCES.get(route)
    if cls is None:
        raise ValueError(f"unknown quote source: {route!r}")
    if route == "jvquant":
        return cls(token or getattr(secrets, "JVQUANT_TOKEN", ""))
    return cls()


def build_live(
    strategy: Any,
    gateway: Any,
    route: str = "paper",
    initial_capital: float = 1_000_000,
    executor_config: dict | None = None,
) -> RealTimeTradeEngine:
    config = {"initial_cash": initial_capital, **(executor_config or {})}
    executor = build_executor(route, config)
    return RealTimeTradeEngine(strategy, gateway, executor, initial_capital=initial_capital, market=settings.LIVE_MARKET)


__all__ = [
    "EXECUTORS",
    "GMTradeExecutor",
    "PaperTradeExecutor",
    "QUOTE_SOURCES",
    "build_executor",
    "build_live",
    "build_quote_client",
]