"""Rebuild weather_stations / weather_daily_events / weather_bins from catalog DB."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import text
from sqlmodel import Session

from poly_bot.persistence.db import get_engine
from poly_bot.persistence.schema import Token
from poly_bot.settings import Settings
from poly_bot.weather.coordinates import lookup_coordinates
from poly_bot.weather.parse import (
    city_key_to_display,
    is_daily_temperature_event_slug,
    parse_bin_from_market,
    parse_daily_event_slug,
    parse_forecast_date_from_payload,
    parse_resolution_unit,
    parse_station_name_from_description,
    parse_wunderground_url,
    resolution_source_from_payload,
)
from poly_bot.weather.repo import WeatherRepository
from poly_bot.weather.schema import WeatherBin, WeatherCity, WeatherDailyEvent, WeatherStation

logger = structlog.get_logger(__name__)

_BIN_SUFFIX_RE = re.compile(r"-\d+c(?:orbelow|orabove|orhigher)?$", re.IGNORECASE)


def infer_event_slug_from_market_slug(market_slug: str) -> str | None:
    slug = market_slug.strip().lower()
    stripped = _BIN_SUFFIX_RE.sub("", slug)
    if is_daily_temperature_event_slug(stripped):
        return stripped
    return None


class WeatherCatalogSync:
    """Materialize structured weather catalog from discovery tables."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._engine = get_engine(settings)

    def sync(self, *, reference_date: date | None = None) -> dict[str, int]:
        ref = reference_date or datetime.now(UTC).date()
        horizon = self._settings.weather_forecast_horizon_days
        min_date = ref
        max_date = ref + timedelta(days=horizon)

        stats: dict[str, int] = {
            "stations": 0,
            "cities": 0,
            "daily_events": 0,
            "bins": 0,
            "pruned_events": 0,
            "skipped_markets": 0,
            "coords_backfilled": 0,
        }
        seen_stations: set[str] = set()
        seen_cities: set[str] = set()

        with Session(self._engine) as session:
            rows = self._load_weather_markets(session)
            grouped = self._group_by_event(rows)
            repo = WeatherRepository(session)

            for group_key, markets in grouped.items():
                event_row = self._build_event_group(group_key, markets)
                if event_row is None:
                    stats["skipped_markets"] += len(markets)
                    continue

                event_id, event_slug, event_raw, series_slug = event_row
                parsed_slug = parse_daily_event_slug(event_slug)
                if parsed_slug is None:
                    forecast_date = parse_forecast_date_from_payload(event_raw)
                    if forecast_date is None:
                        stats["skipped_markets"] += len(markets)
                        continue
                    city_key = _city_from_markets(markets) or "unknown"
                    display_city = city_key_to_display(city_key)
                else:
                    forecast_date = parsed_slug.forecast_date
                    city_key = parsed_slug.city_key
                    display_city = parsed_slug.display_city

                if forecast_date < min_date or forecast_date > max_date:
                    stats["skipped_markets"] += len(markets)
                    continue

                station = self._station_from_payload(
                    event_raw,
                    markets[0],
                    city_display=display_city,
                )
                if station is None:
                    stats["skipped_markets"] += len(markets)
                    continue

                repo.upsert_station(station)
                if station.id not in seen_stations:
                    seen_stations.add(station.id)
                    stats["stations"] += 1

                city = WeatherCity(
                    city_key=city_key,
                    display_name=display_city,
                    series_slug=series_slug,
                    default_station_id=station.id,
                    market_count=len(markets),
                )
                repo.upsert_city(city)
                if city_key not in seen_cities:
                    seen_cities.add(city_key)
                    stats["cities"] += 1

                days_ahead = (forecast_date - ref).days
                description = str(
                    event_raw.get("description") or markets[0].get("description") or ""
                )
                daily = WeatherDailyEvent(
                    id=event_id,
                    event_slug=event_slug,
                    city_key=city_key,
                    station_id=station.id,
                    forecast_date=forecast_date,
                    days_ahead=days_ahead,
                    title=str(event_raw.get("title") or event_raw.get("question") or ""),
                    resolution_rules_text=description,
                    resolution_source_url=station.wunderground_url,
                    neg_risk_market_id=str(
                        event_raw.get("negRiskMarketID")
                        or markets[0].get("neg_risk_market_id")
                        or ""
                    )
                    or None,
                    series_id=str(
                        markets[0].get("series_id") or event_raw.get("seriesId") or ""
                    )
                    or None,
                    end_ts=_parse_dt(
                        event_raw.get("endDate") or markets[0].get("end_ts")
                    ),
                    active=bool(event_raw.get("active", markets[0].get("active", True))),
                    closed=bool(event_raw.get("closed", markets[0].get("closed", False))),
                    bin_count=len(markets),
                    market_count=len(markets),
                )
                repo.upsert_daily_event(daily)
                stats["daily_events"] += 1

                token_map = self._token_map(session, [m["condition_id"] for m in markets])
                for m in markets:
                    raw = m.get("raw_payload") or {}
                    if isinstance(raw, str):
                        raw = {}
                    bin_parsed = parse_bin_from_market(
                        event_slug=event_slug,
                        market_slug=m["slug"],
                        question=m["question"],
                        group_item_title=str(
                            raw.get("groupItemTitle") or raw.get("group_item_title") or ""
                        ),
                    )
                    if bin_parsed is None:
                        continue
                    yes_id, no_id = token_map.get(m["condition_id"], (None, None))
                    bin_row = WeatherBin(
                        condition_id=m["condition_id"],
                        weather_event_id=event_id,
                        slug=m["slug"],
                        question=m["question"],
                        bin_label=bin_parsed.bin_label,
                        bin_index=_as_int(
                            raw.get("groupItemThreshold")
                            or raw.get("group_item_threshold")
                            or 0
                        ),
                        bin_kind=bin_parsed.bin_kind,
                        temp_c=bin_parsed.temp_c,
                        temp_c_upper=bin_parsed.temp_c_upper,
                        temp_c_lower=bin_parsed.temp_c_lower,
                        active=bool(m.get("active", True)),
                        closed=bool(m.get("closed", False)),
                        yes_token_id=yes_id,
                        no_token_id=no_id,
                        group_item_title=str(
                            raw.get("groupItemTitle") or raw.get("group_item_title") or ""
                        ),
                        raw_payload=raw if isinstance(raw, dict) else {},
                    )
                    repo.upsert_bin(bin_row)
                    stats["bins"] += 1

            stats["pruned_events"] = repo.delete_events_outside_horizon(
                min_date=min_date,
                max_date=max_date,
            )
            stats["coords_backfilled"] = self._backfill_station_coordinates(session)
            stats["days_ahead_refreshed"] = self._refresh_days_ahead(
                session, ref=ref, min_date=min_date, max_date=max_date
            )
            session.commit()

        logger.info(
            "weather_catalog_sync_complete",
            reference_date=ref.isoformat(),
            horizon_days=horizon,
            **stats,
        )
        return stats

    def _backfill_station_coordinates(self, session: Session) -> int:
        """Fill lat/lon when ICAO is in the lookup table but DB row was created earlier."""
        from sqlmodel import col, select

        from poly_bot.weather.schema import WeatherStation

        updated = 0
        for st in session.exec(
            select(WeatherStation).where(
                col(WeatherStation.latitude).is_(None) | col(WeatherStation.longitude).is_(None)
            )
        ):
            lat, lon, tz = lookup_coordinates(
                icao=st.icao,
                country_code=st.country_code,
                city_display=st.city_display,
            )
            if lat is None or lon is None:
                continue
            st.latitude = lat
            st.longitude = lon
            if tz != "UTC":
                st.timezone = tz
            session.add(st)
            updated += 1
        return updated

    def _refresh_days_ahead(
        self,
        session: Session,
        *,
        ref: date,
        min_date: date,
        max_date: date,
    ) -> int:
        """Keep stored days_ahead aligned with sync reference (UTC calendar day)."""
        from sqlmodel import select

        from poly_bot.weather.schema import WeatherDailyEvent

        updated = 0
        for ev in session.exec(
            select(WeatherDailyEvent)
            .where(WeatherDailyEvent.forecast_date >= min_date)
            .where(WeatherDailyEvent.forecast_date <= max_date)
        ):
            ahead = (ev.forecast_date - ref).days
            if ev.days_ahead != ahead:
                ev.days_ahead = ahead
                session.add(ev)
                updated += 1
        return updated

    def _load_weather_markets(self, session: Session) -> list[dict[str, Any]]:
        dialect = session.get_bind().dialect.name
        active_clause = "m.active = 1" if dialect == "sqlite" else "m.active IS TRUE"
        stmt = text(
            f"""
            SELECT
                m.condition_id,
                m.slug,
                m.question,
                m.event_id,
                m.series_id,
                m.active,
                m.closed,
                m.end_ts,
                m.neg_risk_market_id,
                m.raw_payload AS market_raw,
                e.id AS event_id_join,
                e.slug AS event_slug,
                e.title AS event_title,
                e.raw_payload AS event_raw,
                s.slug AS series_slug
            FROM markets m
            LEFT JOIN events e ON e.id = m.event_id
            LEFT JOIN series s ON s.id = COALESCE(m.series_id, e.series_id)
            WHERE m.domain = 'weather'
              AND m.slug LIKE 'highest-temperature-%'
              AND {active_clause}
            """
        )

        out: list[dict[str, Any]] = []
        for row in session.exec(stmt).all():
            mapping = row._mapping if hasattr(row, "_mapping") else row
            market_raw = _coerce_json(mapping.get("market_raw"))
            event_raw = _coerce_json(mapping.get("event_raw"))
            out.append(
                {
                    "condition_id": mapping["condition_id"],
                    "slug": mapping["slug"],
                    "question": mapping["question"] or "",
                    "event_id": mapping["event_id"] or mapping.get("event_id_join"),
                    "series_id": mapping["series_id"],
                    "active": mapping["active"],
                    "closed": mapping["closed"],
                    "end_ts": mapping["end_ts"],
                    "neg_risk_market_id": mapping["neg_risk_market_id"],
                    "raw_payload": market_raw,
                    "description": market_raw.get("description") or event_raw.get("description"),
                    "event_slug": mapping.get("event_slug"),
                    "event_raw": event_raw,
                    "series_slug": mapping.get("series_slug"),
                }
            )
        return out

    def _group_by_event(
        self, rows: list[dict[str, Any]]
    ) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            event_slug = row.get("event_slug") or infer_event_slug_from_market_slug(
                row["slug"]
            )
            event_id = row.get("event_id")
            if not event_id and event_slug:
                event_id = event_slug
            if not event_id:
                continue
            key = str(event_id)
            row["_resolved_event_slug"] = event_slug or infer_event_slug_from_market_slug(
                row["slug"]
            )
            grouped[key].append(row)
        return grouped

    def _build_event_group(
        self, group_key: str, markets: list[dict[str, Any]]
    ) -> tuple[str, str, dict[str, Any], str | None] | None:
        event_slug = markets[0].get("_resolved_event_slug") or markets[0].get(
            "event_slug"
        )
        if not event_slug:
            event_slug = infer_event_slug_from_market_slug(markets[0]["slug"])
        if not event_slug:
            return None
        event_id = str(markets[0].get("event_id") or group_key)
        event_raw = markets[0].get("event_raw") or {}
        if not event_raw:
            event_raw = {"slug": event_slug, "title": markets[0].get("question", "")}
        series_slug = markets[0].get("series_slug")
        return event_id, event_slug, event_raw, series_slug

    def _station_from_payload(
        self,
        event_raw: dict[str, Any],
        market_row: dict[str, Any],
        *,
        city_display: str,
    ) -> WeatherStation | None:
        market_raw = market_row.get("raw_payload") or {}
        url = resolution_source_from_payload(event_raw) or resolution_source_from_payload(
            market_raw
        )
        wu = parse_wunderground_url(url)
        if wu is None:
            return None
        description = str(
            event_raw.get("description") or market_row.get("description") or ""
        )
        station_name = parse_station_name_from_description(description)
        lat, lon, tz = lookup_coordinates(
            icao=wu.icao,
            country_code=wu.country_code,
            city_display=city_display,
        )
        unit = parse_resolution_unit(description)
        return WeatherStation(
            id=wu.station_id,
            wunderground_path=wu.wunderground_path,
            wunderground_url=wu.wunderground_url,
            station_name=station_name,
            icao=wu.icao,
            city_display=city_display,
            region=wu.region,
            country_code=wu.country_code,
            latitude=lat,
            longitude=lon,
            timezone=tz,
            unit=unit,
            precision="whole",
            raw_resolution_source=url,
        )

    def _token_map(
        self, session: Session, condition_ids: list[str]
    ) -> dict[str, tuple[str | None, str | None]]:
        if not condition_ids:
            return {}
        from sqlmodel import col, select

        stmt = select(Token).where(col(Token.condition_id).in_(condition_ids))
        rows = session.exec(stmt).all()
        out: dict[str, tuple[str | None, str | None]] = {}
        for t in rows:
            yes, no = out.get(t.condition_id, (None, None))
            if t.outcome_index == 0:
                yes = t.token_id
            elif t.outcome_index == 1:
                no = t.token_id
            out[t.condition_id] = (yes, no)
        return out


def _city_from_markets(markets: list[dict[str, Any]]) -> str | None:
    for m in markets:
        inferred = infer_event_slug_from_market_slug(m["slug"])
        if inferred:
            parsed = parse_daily_event_slug(inferred)
            if parsed:
                return parsed.city_key
    return None


def _coerce_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
