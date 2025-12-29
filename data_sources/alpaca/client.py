from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional

import aiohttp

from core.logging import get_logger

logger = get_logger(__name__)


class AlpacaClient:
    def __init__(self, api_key: str, secret_key: str, env: str = "paper", data_feed: Optional[str] = None) -> None:
        self.api_key = api_key
        self.secret_key = secret_key
        self.env = env
        self.data_feed = data_feed or "sip"
        self.base_url = "https://paper-api.alpaca.markets" if env == "paper" else "https://api.alpaca.markets"
        self.market_data_url = "https://stream.data.alpaca.markets/v2"

    async def rest_get(self, path: str) -> Dict[str, Any]:
        headers = {"APCA-API-KEY-ID": self.api_key, "APCA-API-SECRET-KEY": self.secret_key}
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}{path}", headers=headers) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def ping(self) -> bool:
        try:
            await self.rest_get("/v2/account")
            logger.info("Alpaca REST ping ok")
            return True
        except Exception as exc:  # pragma: no cover - network path
            logger.error("Alpaca REST ping failed", exc_info=exc)
            return False

    async def build_ws_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Basic {self.api_key}:{self.secret_key}"}

