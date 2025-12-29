from __future__ import annotations

import abc
from typing import Any


class Notifier(abc.ABC):
    @abc.abstractmethod
    async def notify(self, message: str) -> None:  # pragma: no cover - interface
        ...
