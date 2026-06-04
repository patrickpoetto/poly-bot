"""Tests for weather slug / resolution parsing."""

from datetime import date

from poly_bot.weather.market_window import (
    country_flag_emoji,
    days_ahead_of_today,
    format_days_ahead_label,
)
from poly_bot.weather.parse import (
    infer_event_slug_from_market_slug,
    parse_bin_from_market,
    parse_daily_event_slug,
    parse_resolution_unit,
    parse_station_name_from_description,
    parse_wunderground_url,
)


def test_country_flag_emoji() -> None:
    assert country_flag_emoji("gb") == "🇬🇧"
    assert country_flag_emoji("US") == "🇺🇸"
    assert country_flag_emoji("") == ""


def test_days_ahead_oggi() -> None:
    ref = date(2026, 6, 4)
    assert days_ahead_of_today(date(2026, 6, 4), ref=ref) == 0
    assert format_days_ahead_label(date(2026, 6, 4), ref=ref) == "oggi"


def test_days_ahead_domani() -> None:
    ref = date(2026, 6, 4)
    assert format_days_ahead_label(date(2026, 6, 5), ref=ref) == "domani"
    assert format_days_ahead_label(date(2026, 6, 6), ref=ref) == "+2g"


def test_parse_daily_event_slug() -> None:
    p = parse_daily_event_slug("highest-temperature-in-buenos-aires-on-may-30-2026")
    assert p is not None
    assert p.city_key == "buenos-aires"
    assert p.forecast_date == date(2026, 5, 30)
    assert p.display_city == "Buenos Aires"


def test_infer_event_slug_from_market() -> None:
    slug = "highest-temperature-in-london-on-may-30-2026-22corbelow"
    assert infer_event_slug_from_market_slug(slug) == "highest-temperature-in-london-on-may-30-2026"


def test_parse_bin_at_or_below() -> None:
    event = "highest-temperature-in-london-on-may-30-2026"
    b = parse_bin_from_market(
        event_slug=event,
        market_slug=f"{event}-22corbelow",
        question="Will the highest temperature in London be 22°C or below on May 30?",
        group_item_title="22°C or below",
    )
    assert b is not None
    assert b.bin_kind == "at_or_below"
    assert b.temp_c_upper == 22


def test_parse_bin_exact() -> None:
    event = "highest-temperature-in-seoul-on-may-30-2026"
    b = parse_bin_from_market(
        event_slug=event,
        market_slug=f"{event}-18c",
        question="Will the highest temperature in Seoul be 18°C on May 30?",
        group_item_title="18°C",
    )
    assert b is not None
    assert b.bin_kind == "exact"
    assert b.temp_c == 18


def test_parse_wunderground_url() -> None:
    wu = parse_wunderground_url(
        "https://www.wunderground.com/history/daily/gb/london/EGLC"
    )
    assert wu is not None
    assert wu.icao == "EGLC"
    assert wu.country_code == "gb"
    assert wu.wunderground_path == "gb/london/EGLC"


def test_parse_bin_at_or_higher() -> None:
    event = "highest-temperature-in-london-on-may-30-2026"
    b = parse_bin_from_market(
        event_slug=event,
        market_slug=f"{event}-32corhigher",
        question="Will the highest temperature in London be 32°C or higher on May 30?",
        group_item_title="32°C or higher",
    )
    assert b is not None
    assert b.bin_kind == "at_or_above"
    assert b.temp_c_lower == 32


def test_parse_resolution_unit_ignores_wu_toggle_boilerplate() -> None:
    desc = (
        "highest temperature recorded at the Minister Pistarini Intl Airport Station "
        "in degrees Celsius on 1 Jun '26. "
        "To toggle between Fahrenheit and Celsius, click the gear icon and switch "
        "the Temperature setting between °F and °C. "
        "The resolution source measures temperatures to whole degrees Celsius."
    )
    assert parse_resolution_unit(desc) == "C"


def test_parse_station_name_from_description() -> None:
    desc = (
        "This market will resolve to the temperature range that contains the highest "
        "temperature recorded at the London City Airport Station in degrees Celsius on 30 May."
    )
    assert parse_station_name_from_description(desc) == "London City Airport"
