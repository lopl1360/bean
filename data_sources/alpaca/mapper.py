from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterable, List

from core.models import BarEvent, MarketEvent, QuoteEvent, TradeEvent


def _parse_timestamp(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def map_message(raw: str, source_name: str) -> List[MarketEvent]:
    try:
        payload = json.loads(raw)
    except Exception:
        # Not JSON? ignore
        return []

    events: List[MarketEvent] = []

    if not isinstance(payload, list):
        payload = [payload]

    for item in payload:
        if not isinstance(item, dict):
            continue

        msg_type = item.get("T")

        # Ignore non-data messages:
        # e.g. {"T":"success","msg":"connected"} / {"T":"error","code":401,...}
        if msg_type in {"success", "error", "subscription"} or msg_type is None:
            continue

        try:
            if msg_type == "t":
                # Trade
                events.append(
                    TradeEvent(
                        symbol=item["S"],
                        price=float(item["p"]),
                        size=float(item.get("s", 0.0)),
                        timestamp=_parse_timestamp(item["t"]),
                        source_name=source_name,
                        raw=item,
                    )
                )

            elif msg_type == "q":
                # Quote
                events.append(
                    QuoteEvent(
                        symbol=item["S"],
                        bid=float(item.get("bp", 0.0)),
                        ask=float(item.get("ap", 0.0)),
                        timestamp=_parse_timestamp(item["t"]),
                        source_name=source_name,
                        raw=item,
                    )
                )

            elif msg_type == "b":
                # Bar
                # Alpaca bar timeframe keys can vary; try a few.
                tf = item.get("tfn") or item.get("tf") or item.get("timeframe") or "1Min"

                # Normalize: "1Min" -> "1m", "5Min" -> "5m"
                if isinstance(tf, str):
                    tf_norm = tf.strip()
                    tf_norm = tf_norm.replace("Min", "m").replace("min", "m")
                    # common variants:
                    # "1Min" -> "1m", "1m" stays, "1Minute" would remain unchanged (fine)
                    tf = tf_norm

                events.append(
                    BarEvent(
                        symbol=item["S"],
                        timeframe=str(tf),
                        open=float(item["o"]),
                        high=float(item["h"]),
                        low=float(item["l"]),
                        close=float(item["c"]),
                        volume=float(item.get("v", 0.0)),
                        timestamp=_parse_timestamp(item["t"]),
                        source_name=source_name,
                        raw=item,
                    )
                )

            else:
                # Unknown message type
                continue

        except KeyError:
            # Missing required fields like S/t/o/h/l/c etc. Skip quietly.
            continue
        except Exception:
            # Bad types or timestamp parsing; skip the malformed message.
            continue

    return events
