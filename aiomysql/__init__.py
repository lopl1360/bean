import asyncio
from contextlib import asynccontextmanager
from typing import Any, Optional


class FakeCursor:
    async def execute(self, query: str, params: Any = None) -> None:
        return None

    async def fetchall(self) -> list:
        return []

    async def fetchone(self) -> Optional[tuple]:
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class Connection:
    def __init__(self, pool: "Pool") -> None:
        self.pool = pool

    @asynccontextmanager
    async def cursor(self):
        yield FakeCursor()


class Pool:
    def __init__(self) -> None:
        self._conn = Connection(self)

    async def acquire(self) -> Connection:
        return self._conn

    def release(self, conn: Connection) -> None:
        return None

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


async def create_pool(*args, **kwargs) -> Pool:
    return Pool()
