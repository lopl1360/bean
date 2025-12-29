from __future__ import annotations

import abc
from typing import AsyncIterator, Iterable, List, Set

from core.models import MarketEvent


class MarketDataSource(abc.ABC):
    name: str

    @abc.abstractmethod
    async def connect(self) -> None:  # pragma: no cover - interface
        ...

    @abc.abstractmethod
    async def subscribe(self, symbols: List[str], channels: Set[str]) -> None:  # pragma: no cover - interface
        ...

    @abc.abstractmethod
    async def stream(self) -> AsyncIterator[MarketEvent]:  # pragma: no cover - interface
        ...

    @abc.abstractmethod
    async def unsubscribe(self, symbols: List[str], channels: Set[str]) -> None:  # pragma: no cover - interface
        ...

    @abc.abstractmethod
    async def close(self) -> None:  # pragma: no cover - interface
        ...
