"""WU API response parsing helpers."""

from __future__ import annotations

from datetime import date

from poly_bot.weather.wu_client import _pick_day_from_arrays


def test_pick_day_from_dailysummary() -> None:
    tmax, tmin = _pick_day_from_arrays(
        valid_times=["2026-06-02T07:00:00+0100", "2026-06-03T07:00:00+0100"],
        temps_max=[18.0, 22.5],
        temps_min=[10.0, 12.0],
        target=date(2026, 6, 2),
        api_units="m",
    )
    assert tmax == 18
    assert tmin == 10
