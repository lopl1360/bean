import asyncio
from datetime import datetime, timezone

from core.models import TradeEvent
from detectors.example_detector import ExamplePriceCrossDetector
from db.repo import InMemoryDedupe


def test_example_detector_triggers():
    detector = ExamplePriceCrossDetector(threshold=50)
    event = TradeEvent(symbol="AAPL", price=55, size=1, timestamp=datetime.now(tz=timezone.utc), source_name="test")
    detections = asyncio.run(detector.on_event(event))
    assert len(detections) == 1
    assert detections[0].symbol == "AAPL"


def test_in_memory_dedupe():
    dedupe = InMemoryDedupe()
    assert dedupe.should_alert("AAPL", "example", cooldown_sec=60) is True
    assert dedupe.should_alert("AAPL", "example", cooldown_sec=60) is False
