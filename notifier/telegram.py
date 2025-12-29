from __future__ import annotations

import asyncio
from typing import Optional

import aiohttp

from core.logging import get_logger
from notifier.base import Notifier

logger = get_logger(__name__)


class TelegramNotifier(Notifier):
    def __init__(self, bot_token: str, chat_id: str, retry_attempts: int = 3, retry_delay: float = 1.0) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay

    async def notify(self, message: str) -> None:
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": message}
        for attempt in range(self.retry_attempts):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload, timeout=10) as resp:
                        if resp.status != 200:
                            raise RuntimeError(f"Telegram returned {resp.status}")
                        logger.info("Telegram message sent")
                        return
            except Exception as exc:  # pragma: no cover - network path
                logger.error("Telegram send failed", exc_info=exc)
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
        raise RuntimeError("Failed to send Telegram message after retries")
