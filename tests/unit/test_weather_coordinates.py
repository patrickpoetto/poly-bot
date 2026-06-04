"""ICAO coordinate lookup for Polymarket weather stations."""

from poly_bot.weather.coordinates import ICAO_COORDINATES, lookup_coordinates

# All ICAOs observed in production weather_stations (2026-05).
_REQUIRED_ICAOS = (
    "ZUUU",
    "ZUCK",
    "KDAL",
    "KBKF",
    "KHOU",
    "OEJN",
    "OPKC",
    "WMKK",
    "VILK",
    "LIMC",
    "KLGA",
    "MPMG",
    "LFPB",
    "ZSQD",
    "ZGSZ",
    "RCSS",
    "NZWN",
    "ZHHH",
)


def test_polymarket_station_icaos_have_coordinates() -> None:
    missing = [code for code in _REQUIRED_ICAOS if code not in ICAO_COORDINATES]
    assert not missing, f"missing ICAO entries: {missing}"


def test_lookup_returns_coords_for_klga() -> None:
    lat, lon, tz = lookup_coordinates(icao="KLGA", country_code="us")
    assert lat is not None and lon is not None
    assert tz == "America/New_York"
