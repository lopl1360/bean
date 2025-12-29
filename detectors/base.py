from __future__ import annotations

import abc
from typing import List, Set

from core.models import Detection, MarketEvent


class Detector(abc.ABC):
    name: str
    required_channels: Set[str]

    @abc.abstractmethod
    async def on_event(self, event: MarketEvent) -> List[Detection]:  # pragma: no cover - interface
        ...

    def warmup_requirements(self) -> dict:
        return {}
