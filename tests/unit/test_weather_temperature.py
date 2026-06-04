"""Temperature unit helpers."""

from __future__ import annotations

from poly_bot.weather.temperature import (
    api_value_to_celsius,
    celsius_to_fahrenheit,
    format_temp_pair,
    round_c_for_resolution,
)


def test_api_metric_to_celsius() -> None:
    assert api_value_to_celsius(22.0, api_units="m") == 22.0


def test_api_imperial_to_celsius() -> None:
    assert round_c_for_resolution(api_value_to_celsius(86.0, api_units="e")) == 30


def test_format_temp_pair_c() -> None:
    assert format_temp_pair(26.4, resolution_unit="C") == "26°C"


def test_format_temp_pair_f() -> None:
    label = format_temp_pair(30.0, resolution_unit="F")
    assert "°F" in label
    assert "30°C" in label or "86°F" in label


def test_celsius_to_fahrenheit() -> None:
    assert int(celsius_to_fahrenheit(0)) == 32
