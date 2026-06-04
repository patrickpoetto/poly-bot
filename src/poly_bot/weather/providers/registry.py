"""Resolve forecast providers by source_key."""

from __future__ import annotations

from poly_bot.weather.providers.aviation import (
    AviationMetarRunningProvider,
    AviationTafProvider,
)
from poly_bot.weather.providers.base import ForecastProvider
from poly_bot.weather.providers.open_meteo import OpenMeteoMultiProvider

_PROVIDERS: dict[str, ForecastProvider] = {
    OpenMeteoMultiProvider.source_key: OpenMeteoMultiProvider(),
    AviationTafProvider.source_key: AviationTafProvider(),
    AviationMetarRunningProvider.source_key: AviationMetarRunningProvider(),
}


def get_provider(source_key: str) -> ForecastProvider | None:
    return _PROVIDERS.get(source_key)


def registered_source_keys() -> list[str]:
    return list(_PROVIDERS.keys())
