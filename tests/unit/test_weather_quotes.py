"""Weather bins included in WS/enrich subscription set."""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlmodel import Session

from poly_bot.catalog.expiring import subscription_condition_ids
from poly_bot.persistence.db import get_engine, init_db
from poly_bot.weather.quotes import weather_condition_ids
from poly_bot.weather.schema import (
    WeatherBin,
    WeatherCity,
    WeatherDailyEvent,
    WeatherStation,
)


def _seed(session: Session) -> str:
    session.add(
        WeatherStation(
            id="TEST",
            wunderground_path="x/y/TEST",
            wunderground_url="https://example.com",
        )
    )
    session.add(WeatherCity(city_key="testcity", display_name="Test", default_station_id="TEST"))
    session.add(
        WeatherDailyEvent(
            id="wev1",
            event_slug="highest-temperature-in-testcity-on-may-31-2026",
            city_key="testcity",
            station_id="TEST",
            forecast_date=date(2026, 5, 31),
            days_ahead=1,
            title="Test",
            end_ts=datetime(2026, 5, 31, 12, 0, tzinfo=UTC),
            bin_count=1,
            market_count=1,
        )
    )
    cid = "0xweather_test_condition"
    session.add(
        WeatherBin(
            condition_id=cid,
            weather_event_id="wev1",
            slug="highest-temperature-in-testcity-on-may-31-2026-20c",
            question="Q",
            bin_label="20C",
            yes_token_id="111",
            no_token_id="222",
        )
    )
    session.commit()
    return cid


def test_weather_condition_ids(settings) -> None:
    init_db(get_engine(settings))
    engine = get_engine(settings)
    with Session(engine) as session:
        cid = _seed(session)
        ids = weather_condition_ids(session, settings)
    assert cid in ids


def test_subscription_includes_weather(settings) -> None:
    init_db(get_engine(settings))
    engine = get_engine(settings)
    with Session(engine) as session:
        cid = _seed(session)
        ids = subscription_condition_ids(session, settings, hours=6.0)
    assert cid in ids
