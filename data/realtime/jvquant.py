from __future__ import annotations

import asyncio
import logging
import threading
import zlib
from typing import Callable, Optional

import requests
import websockets

from data.realtime.base import Level1Tick

logger = logging.getLogger(__name__)


class JvQuantQuoteClient:
    SERVER_URL = "http://jvquant.com/query/server"
    RECONNECT_DELAY = 3.0
    MAX_RECONNECT_DELAY = 30.0
    PING_INTERVAL = 20
    PING_TIMEOUT = 10
    REQUEST_TIMEOUT = 10

    def __init__(self, token: str) -> None:
        self._token = token
        self._ws_url: Optional[str] = None
        self._subscribed: set[str] = set()
        self._on_tick: Optional[Callable[[Level1Tick], None]] = None
        self._on_connect: Optional[Callable[[], None]] = None
        self._on_disconnect: Optional[Callable[[], None]] = None
        self._on_error: Optional[Callable[[Exception], None]] = None
        self._auto_reconnect = True
        self._thread: Optional[threading.Thread] = None

    @property
    def on_tick(self) -> Optional[Callable[[Level1Tick], None]]:
        return self._on_tick

    @on_tick.setter
    def on_tick(self, fn: Callable[[Level1Tick], None]) -> None:
        self._on_tick = fn

    def subscribe(self, *codes: str) -> None:
        self._subscribed.update(f"lv1_{code}" for code in codes)

    def unsubscribe(self, *codes: str) -> None:
        for code in codes:
            self._subscribed.discard(f"lv1_{code}")

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._auto_reconnect = True
        self._thread = threading.Thread(target=self._run_async, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._auto_reconnect = False

    def _run_async(self) -> None:
        try:
            asyncio.run(self._serve())
        except Exception as error:
            logger.error("jvquant worker failed: %s", error)
            if self._on_error is not None:
                self._on_error(error)

    async def _serve(self) -> None:
        delay = self.RECONNECT_DELAY
        while self._auto_reconnect:
            try:
                await self._connect_and_receive()
                if self._auto_reconnect:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self.MAX_RECONNECT_DELAY)
            except asyncio.CancelledError:
                break
            except Exception as error:
                logger.warning("jvquant connect failed: %s", error)
                if self._on_error is not None:
                    self._on_error(error)
                if self._auto_reconnect:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self.MAX_RECONNECT_DELAY)

    async def _connect_and_receive(self) -> None:
        url = self._resolve_server_url()
        async with websockets.connect(
            url, ping_interval=self.PING_INTERVAL, ping_timeout=self.PING_TIMEOUT
        ) as ws:
            logger.info("jvquant WebSocket connected")
            if self._on_connect is not None:
                self._on_connect()
            if self._subscribed:
                await ws.send(f"all={','.join(self._subscribed)}")
            async for message in ws:
                self._dispatch(self._decompress(message))
        if self._on_disconnect is not None:
            self._on_disconnect()

    def _resolve_server_url(self) -> str:
        if self._ws_url is not None:
            return self._ws_url
        resp = requests.get(
            self.SERVER_URL,
            params={"market": "ab", "type": "websocket", "token": self._token},
            timeout=self.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != "0" or not data.get("server"):
            raise RuntimeError(f"jvquant server lookup failed: {data}")
        server = data["server"]
        if not server.startswith(("ws://", "wss://")):
            server = f"ws://{server}"
        self._ws_url = f"{server}?token={self._token}"
        return self._ws_url

    @staticmethod
    def _decompress(data: bytes | str) -> str:
        if isinstance(data, str):
            return data
        try:
            return zlib.decompress(data, -zlib.MAX_WBITS).decode("utf-8")
        except Exception:
            return data.decode("utf-8")

    def _dispatch(self, text: str) -> None:
        if self._on_tick is None:
            return
        for line in text.strip().splitlines():
            if line.startswith("lv1_"):
                tick = self._parse_level1(line)
                if tick is not None:
                    self._on_tick(tick)

    @staticmethod
    def _parse_level1(line: str) -> Optional[Level1Tick]:
        try:
            left, _, right = line.partition("=")
            code = left[4:]
            values = right.split(",")
            if len(values) < 6:
                return None
            trade_date, time = JvQuantQuoteClient._split_time(values[0])
            return Level1Tick(
                code=code,
                name=values[1],
                time=time,
                price=float(values[2]),
                change=float(values[3]),
                volume=int(values[4]),
                amount=float(values[5]),
                bid5=JvQuantQuoteClient._depth(values[6:16]),
                ask5=JvQuantQuoteClient._depth(values[16:26]),
                trade_date=trade_date,
            )
        except Exception:
            return None

    @staticmethod
    def _split_time(raw: str) -> tuple[Optional[str], str]:
        value = raw.strip()
        if len(value) == 14 and value.isdigit():
            return value[:8], f"{value[8:10]}:{value[10:12]}:{value[12:14]}"
        if "T" in value:
            date_part, time_part = value.split("T", 1)
            return date_part.replace("-", ""), time_part
        if " " in value:
            date_part, time_part = value.split(" ", 1)
            return date_part.replace("-", ""), time_part
        return None, value

    @staticmethod
    def _depth(slots: list[str]) -> list[tuple[int, float]]:
        depth = []
        for i in range(0, min(len(slots), 10), 2):
            depth.append((int(slots[i]), float(slots[i + 1])))
        return depth


__all__ = ["JvQuantQuoteClient"]