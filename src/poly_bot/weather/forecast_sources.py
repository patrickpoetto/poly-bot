"""Built-in forecast source registry."""

from __future__ import annotations

from poly_bot.weather.forecast_schema import WeatherForecastSource

BUILTIN_SOURCES: tuple[WeatherForecastSource, ...] = (
    WeatherForecastSource(
        source_key="open_meteo_blend",
        display_name="Open-Meteo multi-model blend",
        provider_kind="common",
        horizon_hours=120,
        enabled=True,
        config_schema={
            "api": "open-meteo.com",
            "models": ["ecmwf_ifs025", "gfs_seamless", "icon_seamless"],
            "method": "mean_of_available_models",
        },
    ),
    WeatherForecastSource(
        source_key="open_meteo_ecmwf",
        display_name="Open-Meteo ECMWF IFS",
        provider_kind="common",
        horizon_hours=120,
        enabled=True,
        config_schema={"model": "ecmwf_ifs025"},
    ),
    WeatherForecastSource(
        source_key="open_meteo_gfs",
        display_name="Open-Meteo GFS",
        provider_kind="common",
        horizon_hours=120,
        enabled=True,
        config_schema={"model": "gfs_seamless"},
    ),
    WeatherForecastSource(
        source_key="open_meteo_icon",
        display_name="Open-Meteo ICON",
        provider_kind="common",
        horizon_hours=120,
        enabled=True,
        config_schema={"model": "icon_seamless"},
    ),
    WeatherForecastSource(
        source_key="merged",
        display_name="Merged selected source",
        provider_kind="common",
        horizon_hours=120,
        enabled=True,
        config_schema={"description": "internal selected forecast used by trading signal"},
    ),
    WeatherForecastSource(
        source_key="aviation_taf",
        display_name="Aviation TAF (TX/TN)",
        provider_kind="common",
        horizon_hours=30,
        enabled=True,
        config_schema={"api": "aviationweather.gov", "product": "taf"},
    ),
    WeatherForecastSource(
        source_key="aviation_metar_running",
        display_name="METAR running daily max",
        provider_kind="common",
        horizon_hours=0,
        enabled=True,
        config_schema={"api": "aviationweather.gov", "product": "metar", "same_day_only": True},
    ),
    WeatherForecastSource(
        source_key="station_override",
        display_name="Per-station custom provider",
        provider_kind="station_override",
        horizon_hours=72,
        enabled=False,
        config_schema={"bind_via": "weather_station_forecast_config.extra_sources"},
    ),
)
