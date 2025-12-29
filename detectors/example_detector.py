from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from core.models import Detection, MarketEvent, TradeEvent
from detectors.base import Detector


class ExamplePriceCrossDetector(Detector):
    name = "example_price_cross"
    required_channels = {"trades"}

    def __init__(self, threshold: float = 100.0) -> None:
        self.threshold = threshold

    async def on_event(self, event: MarketEvent) -> List[Detection]:
        if not isinstance(event, TradeEvent):
            return []
        if event.price <= self.threshold:
            return []
        detection = Detection(
            symbol=event.symbol,
            detector_name=self.name,
            severity="info",
            message=f"Price crossed above {self.threshold}: {event.price}",
            timestamp=event.timestamp,
            data={"price": event.price},
        )
        return [detection]
