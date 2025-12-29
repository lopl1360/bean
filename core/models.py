from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class MarketEvent:
    symbol: str
    timestamp: datetime
    source_name: str
    raw: Optional[Dict[str, Any]] = field(default=None, kw_only=True)


@dataclass
class QuoteEvent(MarketEvent):
    bid: float
    ask: float


@dataclass
class TradeEvent(MarketEvent):
    price: float
    size: float


@dataclass
class BarEvent(MarketEvent):
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class Detection:
    symbol: str
    detector_name: str
    severity: str
    message: str
    timestamp: datetime
    data: Dict[str, Any]
