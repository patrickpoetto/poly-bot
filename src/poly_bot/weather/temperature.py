"""Temperature unit helpers — resolution °C/°F vs API metric/imperial."""

from __future__ import annotations

import math
from datetime import datetime
from zoneinfo import ZoneInfo


def fahrenheit_to_celsius(f: float) -> float:
    return (f - 32.0) * 5.0 / 9.0


def celsius_to_fahrenheit(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def api_value_to_celsius(value: float, *, api_units: str) -> float:
    """Convert a temperature from weather.com when units=m (°C) or e (°F)."""
    if api_units == "e":
        return fahrenheit_to_celsius(value)
    return float(value)


def round_c_for_resolution(c: float) -> int:
    """Whole-degree Celsius for bin comparison (matches typical WU whole °C)."""
    return int(math.floor(c + 0.5))


def format_temp_pair(c: float | None, *, resolution_unit: str) -> str:
    """Human label: resolution unit primary, Celsius in parentheses when F."""
    if c is None:
        return "—"
    c_int = round_c_for_resolution(c)
    if resolution_unit.upper() == "F":
        f_int = int(math.floor(celsius_to_fahrenheit(c) + 0.5))
        return f"{f_int}°F ({c_int}°C)"
    return f"{c_int}°C"


def local_hour_label(iso_local: str, timezone: str) -> str:
    """HH:MM in station timezone from API local ISO timestamp."""
    if not iso_local:
        return ""
    try:
        dt = datetime.fromisoformat(iso_local.replace("Z", "+00:00"))
    except ValueError:
        return ""
    try:
        tz = ZoneInfo(timezone)
    except Exception:
        tz = ZoneInfo("UTC")
    if dt.tzinfo is None:
        return dt.strftime("%H:%M")
    return dt.astimezone(tz).strftime("%H:%M")
