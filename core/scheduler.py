import asyncio
from collections.abc import Awaitable, Callable
from typing import Any


async def periodic(task: Callable[[], Awaitable[Any]], interval_sec: int) -> None:
    while True:
        await task()
        await asyncio.sleep(interval_sec)
