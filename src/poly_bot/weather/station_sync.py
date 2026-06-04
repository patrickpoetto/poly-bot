"""Sync Wunderground resolution-station temps into weather_station_wu_snapshots."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime, timedelta

import httpx
from sqlmodel import Session, col, select

from poly_bot.settings import Settings
from poly_bot.weather.repo import WeatherRepository
from poly_bot.weather.schema import WeatherDailyEvent, WeatherStation, WeatherStationWuSnapshot
from poly_bot.weather.wu_client import fetch_station_reading

log = logging.getLogger(__name__)


class WeatherStationSnapshotSync:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def sync_horizon(self, session: Session) -> dict[str, int]:
        ref = date.today()
        max_d = ref + timedelta(days=self._settings.weather_forecast_horizon_days)
        pairs = session.exec(
            select(WeatherDailyEvent.station_id, WeatherDailyEvent.forecast_date)
            .where(WeatherDailyEvent.forecast_date >= ref)
            .where(WeatherDailyEvent.forecast_date <= max_d)
            .where(WeatherDailyEvent.active == True)  # noqa: E712
            .distinct()
        ).all()
        return asyncio.run(self._sync_pairs(session, pairs))

    def sync_day(self, session: Session, forecast_date: date) -> dict[str, int]:
        pairs = session.exec(
            select(WeatherDailyEvent.station_id, WeatherDailyEvent.forecast_date)
            .where(WeatherDailyEvent.forecast_date == forecast_date)
            .where(WeatherDailyEvent.active == True)  # noqa: E712
            .distinct()
        ).all()
        return asyncio.run(self._sync_pairs(session, pairs))

    async def _sync_pairs(
        self,
        session: Session,
        pairs: list[tuple[str, date]],
    ) -> dict[str, int]:
        if not pairs:
            return {"stations": 0, "ok": 0, "error": 0}

        repo = WeatherRepository(session)
        sem = asyncio.Semaphore(self._settings.weather_wu_concurrency)
        key_cache: dict[str, str] = {}
        stats = {"stations": len(pairs), "ok": 0, "error": 0, "partial": 0}

        async with httpx.AsyncClient(
            headers={"User-Agent": "poly-bot/1.0"},
            follow_redirects=True,
        ) as client:

            async def one(station_id: str, forecast_date: date) -> None:
                station = repo.get_station(station_id)
                if station is None:
                    stats["error"] += 1
                    return
                async with sem:
                    reading = await fetch_station_reading(
                        self._settings,
                        station,
                        forecast_date=forecast_date,
                        client=client,
                        key_cache=key_cache,
                    )
                self._upsert_snapshot(session, station, forecast_date, reading)
                if reading.error:
                    stats["error"] += 1
                elif reading.day.tmax_c is None and reading.current_temp_c is None:
                    stats["partial"] += 1
                else:
                    stats["ok"] += 1

            await asyncio.gather(*(one(sid, fd) for sid, fd in pairs))

        session.commit()
        log.info("weather_station_wu_sync_done", **stats)
        return stats

    def _upsert_snapshot(
        self,
        session: Session,
        station: WeatherStation,
        forecast_date: date,
        reading: object,
    ) -> None:
        from poly_bot.weather.wu_client import WuStationReading

        assert isinstance(reading, WuStationReading)
        now = datetime.now(UTC)
        hours = ",".join(reading.day.tmax_hours_local)
        status = "ok"
        err = reading.error or ""
        if err:
            status = "error"
        elif reading.day.tmax_c is None and reading.current_temp_c is None:
            status = "partial"

        row = session.get(
            WeatherStationWuSnapshot,
            (station.id, forecast_date),
        )
        if row is None:
            row = WeatherStationWuSnapshot(
                station_id=station.id,
                forecast_date=forecast_date,
            )
        row.current_temp_c = reading.current_temp_c
        row.tmax_c = reading.day.tmax_c
        row.tmin_c = reading.day.tmin_c
        row.tmax_hours_local = hours
        row.status = status
        row.error_message = err[:2000] if err else ""
        row.source = reading.day.source
        row.fetched_at = now
        row.meta = {
            "timezone": station.timezone,
            "unit": station.unit,
            "wunderground_url": station.wunderground_url,
        }
        session.add(row)


def latest_wu_sync_time(session: Session) -> datetime | None:
    row = session.exec(
        select(WeatherStationWuSnapshot.fetched_at)
        .order_by(col(WeatherStationWuSnapshot.fetched_at).desc())
        .limit(1)
    ).first()
    return row
