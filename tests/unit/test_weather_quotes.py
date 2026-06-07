"""Weather bins included in WS/enrich subscription set."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from sqlmodel import Session

from poly_bot.catalog.expiring import subscription_condition_ids
from poly_bot.persistence.db import get_engine, init_db
from poly_bot.persistence.schema import Market
from poly_bot.weather.quotes import weather_condition_ids
from poly_bot.weather.schema import (
    WeatherBin,
    WeatherCity,
    WeatherDailyEvent,
    WeatherStation,
)


def _seed(session: Session) -> str:
    cid = "0xweather_test_condition"
    session.add(Market(condition_id=cid, slug="testcity-temp-20c", question="Q"))
    session.add(
        WeatherStation(
            id="TEST", wunderground_path="x/y/TEST", wunderground_url="https://example.com"
        )
    )
    session.flush()
    session.add(WeatherCity(city_key="testcity", display_name="Test", default_station_id="TEST"))
    session.flush()
    tomorrow = date.today() + timedelta(days=1)
    session.add(
        WeatherDailyEvent(
            id="wev1",
            event_slug=f"highest-temperature-in-testcity-on-{tomorrow.strftime('%B-%d-%Y').lower()}",
            city_key="testcity",
            station_id="TEST",
            forecast_date=tomorrow,
            days_ahead=1,
            title="Test",
            end_ts=datetime(tomorrow.year, tomorrow.month, tomorrow.day, 12, 0, tzinfo=UTC),
            bin_count=1,
            market_count=1,
        )
    )
    session.flush()
    session.add(
        WeatherBin(
            condition_id=cid,
            weather_event_id="wev1",
            slug=f"highest-temperature-in-testcity-on-{tomorrow.strftime('%B-%d-%Y').lower()}-20c",
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
