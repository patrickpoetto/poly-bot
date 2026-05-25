"""Tests for event and series mapping."""

from poly_bot.discovery.mapper import gamma_event_to_entity, gamma_series_to_entity


def test_gamma_event_mapping() -> None:
    raw = {
        "id": "12345",
        "slug": "btc-updown-5m",
        "title": "BTC 5m window",
        "seriesId": "99",
        "active": True,
        "closed": False,
        "tags": [{"slug": "crypto"}],
    }
    event = gamma_event_to_entity(raw)
    assert event is not None
    assert event.id == "12345"
    assert event.series_id == "99"
    assert "crypto" in event.tags_json


def test_gamma_series_mapping() -> None:
    raw = {
        "id": "99",
        "slug": "btc-updown-5m",
        "title": "Bitcoin 5 minute",
        "recurrence": "5m",
    }
    series = gamma_series_to_entity(raw)
    assert series is not None
    assert series.id == "99"
    assert series.recurrence_seconds == 300
