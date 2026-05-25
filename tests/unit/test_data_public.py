"""Tests for Data API response parsing."""

from poly_bot.exchange.data_public import _parse_oi_payload


def test_parse_oi_list_response() -> None:
    data = [{"market": "0xabc", "value": 16126.55034}]
    assert _parse_oi_payload(data) == 16126.55034


def test_parse_oi_scalar() -> None:
    assert _parse_oi_payload(42.5) == 42.5


def test_parse_oi_dict() -> None:
    assert _parse_oi_payload({"openInterest": 100.0}) == 100.0
