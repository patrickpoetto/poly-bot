"""Dashboard weather routes."""

from __future__ import annotations

from datetime import UTC, date, datetime

from fastapi.testclient import TestClient
from sqlmodel import Session

from poly_bot.catalog.dashboard import create_app
from poly_bot.persistence.db import get_engine, init_db
from poly_bot.weather.schema import (
    WeatherBin,
    WeatherCity,
    WeatherDailyEvent,
    WeatherStation,
)


def _seed_weather(session: Session) -> None:
    session.add(
        WeatherStation(
            id="EGLC",
            wunderground_path="gb/london/EGLC",
            wunderground_url="https://www.wunderground.com/history/daily/gb/london/EGLC",
            station_name="London City Airport",
            icao="EGLC",
            city_display="London",
            country_code="gb",
            latitude=51.505,
            longitude=0.055,
            timezone="Europe/London",
        )
    )
    session.add(
        WeatherCity(
            city_key="london",
            display_name="London",
            default_station_id="EGLC",
        )
    )
    session.add(
        WeatherDailyEvent(
            id="ev1",
            event_slug="highest-temperature-in-london-on-may-30-2026",
            city_key="london",
            station_id="EGLC",
            forecast_date=date(2026, 5, 30),
            days_ahead=0,
            title="Highest temperature in London on May 30?",
            resolution_rules_text="Resolve at EGLC.",
            resolution_source_url="https://www.wunderground.com/history/daily/gb/london/EGLC",
            end_ts=datetime(2026, 5, 30, 12, 0, tzinfo=UTC),
            bin_count=2,
            market_count=2,
        )
    )
    session.add(
        WeatherBin(
            condition_id="0xabc",
            weather_event_id="ev1",
            slug="highest-temperature-in-london-on-may-30-2026-22corbelow",
            question="Will the highest temperature in London be 22°C or below on May 30?",
            bin_label="22°C or below",
            bin_index=0,
            bin_kind="at_or_below",
            temp_c_upper=22,
        )
    )
    session.commit()


def test_weather_dashboard_page(settings) -> None:
    init_db(get_engine(settings))
    engine = get_engine(settings)
    with Session(engine) as session:
        _seed_weather(session)

    app = create_app(settings)
    with TestClient(app) as client:
        r = client.get("/weather?day=2026-05-30")
        assert r.status_code == 200
        assert "London" in r.text
        assert "EGLC" in r.text
        assert "card-grid" in r.text
        assert "/api/weather/cards" in r.text

        cards = client.get("/api/weather/cards?day=2026-05-30")
        assert cards.status_code == 200
        body = cards.json()
        assert body["forecast_date"] == "2026-05-30"
        assert len(body["cards"]) == 1

        d = client.get("/weather/event?city=london&day=2026-05-30")
        assert d.status_code == 200
        assert "22°C or below" in d.text
        assert 'data-col="yes"' in d.text
        assert "/api/weather/stream" in d.text

        api = client.get("/api/weather?day=2026-05-30")
        assert api.status_code == 200
        data = api.json()
        assert len(data) == 1
        assert data[0]["city_key"] == "london"
