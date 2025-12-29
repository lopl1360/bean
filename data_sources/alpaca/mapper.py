from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterable, List

from core.models import BarEvent, MarketEvent, QuoteEvent, TradeEvent


def _parse_timestamp(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def map_message(raw: str, source_name: str) -> List[MarketEvent]:
    payload = json.loads(raw)
    events: List[MarketEvent] = []
    if not isinstance(payload, list):
        payload = [payload]
    for item in payload:
        msg_type = item.get("T")
        if msg_type == "t":
            events.append(
                TradeEvent(
                    symbol=item["S"],
                    price=float(item["p"]),
                    size=float(item["s"]),
                    timestamp=_parse_timestamp(item["t"]),
                    source_name=source_name,
                    raw=item,
                )
            )
        elif msg_type == "q":
            events.append(
                QuoteEvent(
                    symbol=item["S"],
                    bid=float(item["bp"]),
                    ask=float(item["ap"]),
                    timestamp=_parse_timestamp(item["t"]),
                    source_name=source_name,
                    raw=item,
                )
            )
        elif msg_type == "b":
            events.append(
                BarEvent(
                    symbol=item["S"],
                    timeframe=item.get("tfn", "1Min"),
                    open=float(item["o"]),
                    high=float(item["h"]),
                    low=float(item["l"]),
                    close=float(item["c"]),
                    volume=float(item["v"]),
                    timestamp=_parse_timestamp(item["t"]),
                    source_name=source_name,
                    raw=item,
                )
            )
    return events
