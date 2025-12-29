import asyncio
import sqlite3
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

try:
    import pymysql  # type: ignore

    _connect = pymysql.connect
except ModuleNotFoundError:  # pragma: no cover - fallback for offline installs
    def _sqlite_connect(**config):
        database = config.get("db") or ":memory:"
        conn = sqlite3.connect(database)
        conn.isolation_level = None  # autocommit
        return conn

    class _SQLiteModule:
        Connection = sqlite3.Connection

    pymysql = _SQLiteModule()  # type: ignore
    _connect = _sqlite_connect


class Cursor:
    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    async def execute(self, query: str, params: Any = None) -> None:
        await asyncio.to_thread(self._cursor.execute, query, params)

    async def fetchall(self) -> list:
        return await asyncio.to_thread(self._cursor.fetchall)

    async def fetchone(self) -> Optional[tuple]:
        return await asyncio.to_thread(self._cursor.fetchone)

    async def close(self) -> None:
        await asyncio.to_thread(self._cursor.close)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        return False


class Connection:
    def __init__(self, pool: "Pool", conn: Any) -> None:
        self.pool = pool
        self._conn = conn

    @asynccontextmanager
    async def cursor(self) -> AsyncIterator[Cursor]:
        cursor = self._conn.cursor()
        wrapper = Cursor(cursor)
        try:
            yield wrapper
        finally:
            await wrapper.close()

    async def close(self) -> None:
        await asyncio.to_thread(self._conn.close)


class Pool:
    def __init__(self, minsize: int = 1, maxsize: int = 5, **config) -> None:
        self._config = config
        self._queue: asyncio.Queue[Connection] = asyncio.Queue(maxsize)
        self._all_conns: list[Connection] = []
        self._minsize = minsize
        self._maxsize = maxsize
        self._initialized = False
        self._closed = False

    async def _initialize(self) -> None:
        for _ in range(self._minsize):
            conn = await self._create_connection()
            await self._queue.put(conn)
        self._initialized = True

    async def _create_connection(self) -> Connection:
        raw_conn = await asyncio.to_thread(_connect, **self._config)
        conn = Connection(self, raw_conn)
        self._all_conns.append(conn)
        return conn

    async def acquire(self) -> Connection:
        if not self._initialized:
            await self._initialize()
        try:
            conn = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            if len(self._all_conns) < self._maxsize:
                conn = await self._create_connection()
            else:
                conn = await self._queue.get()
        return conn

    def release(self, conn: Connection) -> None:
        if not self._closed:
            self._queue.put_nowait(conn)

    def close(self) -> None:
        self._closed = True
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    async def wait_closed(self) -> None:
        await asyncio.gather(*(conn.close() for conn in self._all_conns))


async def create_pool(*args, **kwargs) -> Pool:
    return Pool(*args, **kwargs)
