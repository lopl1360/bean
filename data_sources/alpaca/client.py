from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass
from typing import AsyncIterator, Iterable, List, Optional, Sequence, Set

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from core.logging import get_logger
from data_sources.alpaca.client import AlpacaClient  # adjust import if needed

logger = get_logger(__name__)


# ----------------------------
# Helpers / Models
# ----------------------------

@dataclass(frozen=True)
class Subscription:
    symbols: List[str]
    channels: Set[str]


def _chunked(seq: Sequence[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        raise ValueError("chunk size must be > 0")
    for i in range(0, len(seq), size):
        yield list(seq[i : i + size])


def map_message(raw: str, source_name: str) -> list[object]:
    """
    Convert an Alpaca WS raw message into internal event objects.

    Replace this with your real mapper that returns your internal MarketEvent types.
    For now, this stub keeps the pipeline alive without crashing.

    Alpaca sends JSON lists like:
      [{"T":"t","S":"AAPL","p":..., "s":..., "t":"..."}]
      [{"T":"success","msg":"authenticated"}]
      [{"T":"error","code":401,"msg":"not authenticated"}]
    """
    # TODO: plug in your real mapper.
    _ = (raw, source_name)
    return []


# ----------------------------
# Alpaca WebSocket Data Source
# ----------------------------

class AlpacaWebSocket:
    """
    Market data websocket client for Alpaca (v2 stream).

    Key behavior:
    - Connects to wss://stream.data.alpaca.markets/v2/{feed}
    - Authenticates via WS message: {"action":"auth","key":..., "secret":...}
    - Waits for "authenticated" before subscribing
    - Supports chunked subscriptions (e.g. quotes limit)
    - Optional rotation loop for overflow symbols
    - Reconnects with exponential backoff on disconnects
    """

    name = "alpaca"

    def __init__(
        self,
        client: AlpacaClient,
        max_symbols_per_chunk: int = 200,
        rotation_interval_sec: int = 300,
    ) -> None:
        self._client = client
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

        self._max_symbols_per_chunk = max_symbols_per_chunk
        self._rotation_interval_sec = rotation_interval_sec

        self._all_symbols: List[str] = []
        self._channels: Set[str] = set()

        self._active_chunk_index: int = 0
        self._rotation_task: Optional[asyncio.Task[None]] = None

        self._stop_event = asyncio.Event()

    # ----------------------------
    # URL / Messages
    # ----------------------------

    def _ws_url(self) -> str:
        feed = getattr(self._client, "data_feed", None) or "iex"
        return f"wss://stream.data.alpaca.markets/v2/{feed}"

    def _auth_payload(self) -> str:
        return json.dumps(
            {
                "action": "auth",
                "key": self._client.api_key,
                "secret": self._client.secret_key,
            }
        )

    def _subscribe_payload(self, symbols: List[str], channels: Set[str]) -> str:
        payload: dict[str, object] = {"action": "subscribe"}

        # Alpaca expects channel keys like "trades", "quotes", "bars"
        if "trades" in channels:
            payload["trades"] = symbols
        if "quotes" in channels:
            payload["quotes"] = symbols
        if "bars" in channels:
            payload["bars"] = symbols

        return json.dumps(payload)

    def _unsubscribe_payload(self, symbols: List[str], channels: Set[str]) -> str:
        payload: dict[str, object] = {"action": "unsubscribe"}

        if "trades" in channels:
            payload["trades"] = symbols
        if "quotes" in channels:
            payload["quotes"] = symbols
        if "bars" in channels:
            payload["bars"] = symbols

        return json.dumps(payload)

    # ----------------------------
    # Lifecycle
    # ----------------------------

    async def connect(self) -> None:
        """
        Connect + authenticate.
        """
        url = self._ws_url()
        logger.info(f"Connecting to Alpaca WS: {url}")

        self._ws = await websockets.connect(url)

        # Initial server message is typically: [{"T":"success","msg":"connected"}]
        raw = await self._ws.recv()
        logger.info(f"WS connect response {raw}")

        await self._ws.send(self._auth_payload())
        raw = await self._ws.recv()
        logger.info(f"WS auth response {raw}")

        msgs = json.loads(raw)
        if any(m.get("T") == "error" for m in msgs):
            raise RuntimeError(f"Alpaca WS auth failed: {msgs}")

        if not any(m.get("msg") == "authenticated" for m in msgs):
            raise RuntimeError(f"Unexpected Alpaca WS auth response: {msgs}")

        logger.info("Connected to Alpaca WebSocket (authenticated)")

    async def close(self) -> None:
        self._stop_event.set()

        if self._rotation_task:
            self._rotation_task.cancel()
            self._rotation_task = None

        if self._ws:
            try:
                await self._ws.close()
            except Exception as exc:
                logger.warning("Error closing Alpaca WebSocket", exc_info=exc)
            self._ws = None

        logger.info("Closed Alpaca WebSocket")

    # ----------------------------
    # Subscriptions
    # ----------------------------

    async def subscribe(self, symbols: List[str], channels: Set[str]) -> None:
        """
        Subscribe to symbols/channels.
        If symbol count exceeds max chunk size, subscribe to first chunk and
        rotate through remaining chunks periodically.
        """
        if not symbols:
            logger.warning("No symbols provided to subscribe()")
            self._all_symbols = []
            self._channels = set(channels)
            return

        self._all_symbols = list(dict.fromkeys(symbols))  # dedupe, keep order
        self._channels = set(channels)

        if self._ws is None:
            await self.connect()

        assert self._ws is not None

        self._active_chunk_index = 0
        chunks = list(_chunked(self._all_symbols, self._max_symbols_per_chunk))

        # Subscribe to the first chunk immediately
        first_chunk = chunks[0]
        await self._send_subscribe(first_chunk, self._channels)
        logger.info("Subscribed chunk")

        # Start rotation if overflow
        if len(chunks) > 1 and self._rotation_task is None:
            self._rotation_task = asyncio.create_task(self._rotation_loop())

    async def _send_subscribe(self, symbols: List[str], channels: Set[str]) -> None:
        assert self._ws is not None
        payload = self._subscribe_payload(symbols, channels)
        await self._ws.send(payload)

        # Alpaca usually sends an ack like success/subscription
        raw = await self._ws.recv()
        logger.info(f"WS subscribe response {raw}")

        msgs = json.loads(raw)
        if any(m.get("T") == "error" for m in msgs):
            raise RuntimeError(f"Alpaca WS subscribe failed: {msgs}")

    async def _send_unsubscribe(self, symbols: List[str], channels: Set[str]) -> None:
        assert self._ws is not None
        payload = self._unsubscribe_payload(symbols, channels)
        await self._ws.send(payload)

        raw = await self._ws.recv()
        logger.info(f"WS unsubscribe response {raw}")

        msgs = json.loads(raw)
        if any(m.get("T") == "error" for m in msgs):
            raise RuntimeError(f"Alpaca WS unsubscribe failed: {msgs}")

    async def _rotation_loop(self) -> None:
        """
        Rotate through symbol chunks to respect subscription limits.
        This is a simple approach:
        - unsubscribe active chunk
        - subscribe next chunk
        - sleep
        """
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(self._rotation_interval_sec)

                if not self._all_symbols or not self._channels:
                    continue
                if self._ws is None:
                    continue

                chunks = list(_chunked(self._all_symbols, self._max_symbols_per_chunk))
                if len(chunks) <= 1:
                    continue

                current_chunk = chunks[self._active_chunk_index]
                next_index = (self._active_chunk_index + 1) % len(chunks)
                next_chunk = chunks[next_index]

                logger.info(
                    f"Rotating subscription chunk {self._active_chunk_index} -> {next_index} "
                    f"(size {len(current_chunk)} -> {len(next_chunk)})"
                )

                await self._send_unsubscribe(current_chunk, self._channels)
                await self._send_subscribe(next_chunk, self._channels)

                self._active_chunk_index = next_index

        except asyncio.CancelledError:
            # normal shutdown
            return
        except Exception as exc:
            logger.error("Rotation loop failed", exc_info=exc)
            # Let the main loop handle reconnect logic if needed.

    # ----------------------------
    # Streaming
    # ----------------------------

    async def stream(self) -> AsyncIterator[object]:
        """
        Async iterator of internal events.
        Reconnects on disconnect with exponential backoff.
        """
        backoff = 1.0
        max_backoff = 30.0

        while not self._stop_event.is_set():
            try:
                if self._ws is None:
                    await self.connect()
                    # Re-subscribe after reconnect
                    if self._all_symbols and self._channels:
                        chunks = list(_chunked(self._all_symbols, self._max_symbols_per_chunk))
                        chunk = chunks[self._active_chunk_index] if chunks else []
                        if chunk:
                            await self._send_subscribe(chunk, self._channels)

                assert self._ws is not None

                # Reset backoff after a healthy connect
                backoff = 1.0

                async for raw in self._ws:
                    # Alpaca sends JSON list strings
                    # Handle auth/subscription errors that can appear mid-stream
                    if isinstance(raw, str) and '"T":"error"' in raw:
                        logger.error(f"WS error message: {raw}")
                        # Force reconnect
                        raise ConnectionClosedError(None, None, None)

                    events = map_message(raw, source_name=self.name)
                    for event in events:
                        yield event

            except ConnectionClosedOK:
                logger.info("WS closed normally, reconnecting")
            except ConnectionClosedError as exc:
                logger.warning("WS connection closed unexpectedly, will reconnect", exc_info=exc)
            except Exception as exc:
                logger.error("WS stream loop error, will reconnect", exc_info=exc)
            finally:
                # Ensure socket is closed before reconnecting
                if self._ws is not None:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

            # Backoff before reconnect (with jitter)
            jitter = random.uniform(0.0, 0.5)
            sleep_for = min(max_backoff, backoff) + jitter
            logger.info(f"Reconnecting in {sleep_for:.1f}s")
            await asyncio.sleep(sleep_for)
            backoff = min(max_backoff, backoff * 2)
