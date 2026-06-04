"""Forecast provider protocol — common and per-station sources."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol

from poly_bot.weather.schema import WeatherStation


@dataclass(frozen=True, slots=True)
class DailyForecastPoint:
    station_id: str
    forecast_date: date
    source_key: str
    tmax_value: float | None
    unit: str
    tmax_c: int | None
    coverage: str
    issued_at: datetime | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    meta: dict[str, Any] = field(default_factory=dict)


class ForecastProvider(Protocol):
    source_key: str

    def produce_daily_forecasts(
        self,
        station: WeatherStation,
        forecast_dates: list[date],
        *,
        context: dict[str, Any],
    ) -> list[DailyForecastPoint]:
        """Build daily high forecasts for the given local dates."""
