from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from typing import AsyncIterator, Dict, List, Set

import websockets
from websockets import WebSocketClientProtocol

from core.logging import get_logger
from core.models import MarketEvent
from data_sources.alpaca.mapper import map_message
from data_sources.base import MarketDataSource

logger = get_logger(__name__)


class AlpacaWebSocket(MarketDataSource):
    name = "alpaca"

    def __init__(self, api_key: str, secret_key: str, data_feed: str = "sip", max_quotes: int = 200, rotation_interval: int = 300) -> None:
        self.api_key = api_key
        self.secret_key = secret_key
        self.data_feed = data_feed
        self.max_quotes = max_quotes
        self.rotation_interval = rotation_interval
        self._ws: WebSocketClientProtocol | None = None
        self._symbols: List[str] = []
        self._channels: Set[str] = set()
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()

    async def connect(self) -> None:
        await self._connect_ws()

    async def _connect_ws(self) -> None:
        url = f"wss://stream.data.alpaca.markets/v2/{self.data_feed}"
        backoff = 1
        while not self._stop.is_set():
            try:
                self._ws = await websockets.connect(url, ping_interval=20)
                await self._authenticate()
                if self._symbols:
                    await self._send_subscriptions(self._symbols, self._channels)
                logger.info("Connected to Alpaca WebSocket")
                return
            except Exception as exc:  # pragma: no cover - network path
                logger.error("WebSocket connection failed", exc_info=exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _authenticate(self) -> None:
        assert self._ws
        auth_msg = {"action": "auth", "key": self.api_key, "secret": self.secret_key}
        await self._ws.send(json.dumps(auth_msg))
        response = await self._ws.recv()
        logger.info("WS auth response %s", response)

    async def subscribe(self, symbols: List[str], channels: Set[str]) -> None:
        async with self._lock:
            self._symbols = symbols
            self._channels = channels
            await self._send_subscriptions(symbols, channels)

    async def _send_subscriptions(self, symbols: List[str], channels: Set[str]) -> None:
        if not symbols or not channels or not self._ws:
            return
        chunks = [symbols[i : i + self.max_quotes] for i in range(0, len(symbols), self.max_quotes)]
        for chunk in chunks:
            msg = {"action": "subscribe"}
            for channel in channels:
                msg[channel] = chunk
            await self._ws.send(json.dumps(msg))
            logger.info("Subscribed chunk", extra={"chunk": chunk, "channels": list(channels)})

    async def stream(self) -> AsyncIterator[MarketEvent]:
        if self._ws is None:
            await self._connect_ws()
        assert self._ws
        rotation_task = asyncio.create_task(self._rotation_loop())
        try:
            async for raw in self._ws:
                events = map_message(raw, source_name=self.name)
                for event in events:
                    yield event
        finally:
            rotation_task.cancel()

    async def _rotation_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.rotation_interval)
            if len(self._symbols) <= self.max_quotes:
                continue
            # rotate symbols to avoid exceeding max subscriptions
            rotated = self._symbols[self.max_quotes :] + self._symbols[: self.max_quotes]
            await self.subscribe(rotated, self._channels)
            logger.info("Rotated subscription batch")

    async def unsubscribe(self, symbols: List[str], channels: Set[str]) -> None:
        if not self._ws:
            return
        msg = {"action": "unsubscribe"}
        for channel in channels:
            msg[channel] = symbols
        await self._ws.send(json.dumps(msg))

    async def close(self) -> None:
        self._stop.set()
        if self._ws:
            await self._ws.close()
            logger.info("Closed Alpaca WebSocket")
            self._ws = None
