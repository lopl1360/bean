import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import aiomysql

from core.logging import get_logger

logger = get_logger(__name__)


class MySQLPool:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        db: str,
        minsize: int = 1,
        maxsize: int = 5,
    ) -> None:
        self._pool: Optional[aiomysql.Pool] = None
        self._config = dict(host=host, port=port, user=user, password=password, db=db, minsize=minsize, maxsize=maxsize, autocommit=True)

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await aiomysql.create_pool(**self._config)
            logger.info("MySQL pool created")

    async def close(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            logger.info("MySQL pool closed")
            self._pool = None

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[aiomysql.Connection]:
        if self._pool is None:
            raise RuntimeError("Pool not initialized")
        conn = await self._pool.acquire()
        try:
            yield conn
        finally:
            self._pool.release(conn)

    async def ping(self) -> None:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        async with self.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                await cur.fetchone()
                logger.info("MySQL ping successful")


def create_pool_from_settings(settings) -> MySQLPool:
    return MySQLPool(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        db=settings.mysql_db,
    )
