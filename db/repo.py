from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Sequence

from aiomysql import Connection

from core.logging import get_logger

logger = get_logger(__name__)


class WatchlistRepository:
    def __init__(self, pool) -> None:
        self.pool = pool

    async def list_symbols(self, limit: Optional[int] = None) -> List[str]:
        query = "SELECT symbol FROM watchlist_symbols WHERE is_active = 1 ORDER BY id ASC"
        if limit:
            query += " LIMIT %s"
            params: Sequence = (limit,)
        else:
            params = ()
        async with self.pool.acquire() as conn:  # type: Connection
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                rows = await cur.fetchall()
                return [row[0] for row in rows]

    async def add_symbol(self, symbol: str) -> None:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO watchlist_symbols (symbol, is_active, created_at, updated_at) VALUES (%s, 1, NOW(), NOW()) ON DUPLICATE KEY UPDATE is_active=VALUES(is_active), updated_at=NOW()",
                    (symbol,),
                )
                logger.info("Added symbol to watchlist", extra={"symbol": symbol})

    async def remove_symbol(self, symbol: str) -> None:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE watchlist_symbols SET is_active=0, updated_at=NOW() WHERE symbol=%s",
                    (symbol,),
                )
                logger.info("Removed symbol from watchlist", extra={"symbol": symbol})


class DetectorStateRepository:
    def __init__(self, pool) -> None:
        self.pool = pool

    async def get_state(self, symbol: str, detector_name: str) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT last_triggered_at, last_payload_hash FROM detector_state WHERE symbol=%s AND detector_name=%s",
                    (symbol, detector_name),
                )
                row = await cur.fetchone()
                if row:
                    return {"last_triggered_at": row[0], "last_payload_hash": row[1]}
                return None

    async def upsert_state(self, symbol: str, detector_name: str, payload_hash: str) -> None:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO detector_state (symbol, detector_name, last_triggered_at, last_payload_hash, created_at, updated_at) VALUES (%s, %s, NOW(), %s, NOW(), NOW()) ON DUPLICATE KEY UPDATE last_triggered_at=VALUES(last_triggered_at), last_payload_hash=VALUES(last_payload_hash), updated_at=NOW()",
                    (symbol, detector_name, payload_hash),
                )

    async def should_alert(self, symbol: str, detector_name: str, payload: str, cooldown_sec: int) -> bool:
        state = await self.get_state(symbol, detector_name)
        payload_hash = hashlib.sha256(payload.encode()).hexdigest()
        if not state:
            await self.upsert_state(symbol, detector_name, payload_hash)
            return True
        last_triggered = state.get("last_triggered_at")
        last_hash = state.get("last_payload_hash")
        if last_hash == payload_hash and last_triggered and (datetime.utcnow() - last_triggered).total_seconds() < cooldown_sec:
            return False
        if last_triggered and (datetime.utcnow() - last_triggered).total_seconds() < cooldown_sec:
            return False
        await self.upsert_state(symbol, detector_name, payload_hash)
        return True


class AlertsLogRepository:
    def __init__(self, pool) -> None:
        self.pool = pool

    async def insert_alert(self, symbol: str, detector_name: str, message: str, raw_event_json: str) -> None:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO alerts_log (symbol, detector_name, message, sent_at, raw_event_json, created_at, updated_at) VALUES (%s, %s, %s, NOW(), %s, NOW(), NOW())",
                    (symbol, detector_name, message, raw_event_json),
                )


class InMemoryDedupe:
    def __init__(self) -> None:
        self.last_seen: dict[tuple[str, str], datetime] = {}

    def should_alert(self, symbol: str, detector_name: str, cooldown_sec: int) -> bool:
        key = (symbol, detector_name)
        now = datetime.utcnow()
        last = self.last_seen.get(key)
        if last and (now - last) < timedelta(seconds=cooldown_sec):
            return False
        self.last_seen[key] = now
        return True
