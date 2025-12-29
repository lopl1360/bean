from datetime import datetime, timezone

from data_sources.alpaca.mapper import map_message


def test_map_trade_message():
    raw = '[{"T":"t","S":"AAPL","p":105.5,"s":10,"t":"2023-09-01T12:00:00Z"}]'
    events = map_message(raw, source_name="alpaca")
    assert len(events) == 1
    event = events[0]
    assert event.symbol == "AAPL"
    assert event.price == 105.5
    assert event.timestamp == datetime(2023, 9, 1, 12, 0, tzinfo=timezone.utc)


def test_map_quote_and_bar_message():
    raw = '[{"T":"q","S":"MSFT","bp":100,"ap":101,"t":"2023-09-01T12:00:00Z"},{"T":"b","S":"MSFT","o":1,"h":2,"l":0.5,"c":1.5,"v":1000,"t":"2023-09-01T12:00:00Z","tfn":"1Min"}]'
    events = map_message(raw, source_name="alpaca")
    assert len(events) == 2
    assert events[0].bid == 100
    assert events[1].close == 1.5
