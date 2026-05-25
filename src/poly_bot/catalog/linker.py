"""Link markets to series and tags using events + slug patterns."""

from __future__ import annotations

import re

import structlog
from sqlalchemy import text
from sqlmodel import Session, select

from poly_bot.discovery.classifier.rules import classify_domain_from_tags
from poly_bot.persistence.schema import Event, Market, MarketTag, Series

logger = structlog.get_logger(__name__)

# Hardcoded slug prefix → series.slug (fallback if series table slug differs)
_SLUG_PREFIX_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^btc-updown-5m-"), "btc-updown-5m"),
    (re.compile(r"^btc-updown-15m-"), "btc-updown-15m"),
    (re.compile(r"^eth-updown-5m-"), "eth-updown-5m"),
    (re.compile(r"^eth-updown-15m-"), "eth-updown-15m"),
]


class CatalogLinker:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._series_list = list(self._session.exec(select(Series)).all())
        self._series_by_slug: dict[str, str] = {s.slug: s.id for s in self._series_list}
        self._series_slugs_sorted = sorted(
            (s.slug for s in self._series_list if s.slug),
            key=len,
            reverse=True,
        )

    def link_all(self) -> dict[str, int]:
        series_linked = self._link_series_by_slug()
        tags_synced = self._sync_tags_from_events()
        domains_updated = self._apply_domain_from_events()
        series_backfilled = self._backfill_series_from_events()
        from poly_bot.persistence.repo import CatalogRepository

        event_fees_rolled = CatalogRepository(self._session).rollup_event_fees()
        logger.info(
            "catalog_link_complete",
            series_by_slug=series_linked,
            tags_synced=tags_synced,
            domains_updated=domains_updated,
            series_from_events=series_backfilled,
            event_fees_rolled=event_fees_rolled,
        )
        return {
            "series_by_slug": series_linked,
            "tags_synced": tags_synced,
            "domains_updated": domains_updated,
            "series_from_events": series_backfilled,
            "event_fees_rolled": event_fees_rolled,
        }

    def _link_series_by_slug(self) -> int:
        updated = 0
        for market in self._session.exec(select(Market).where(Market.active == True)).all():  # noqa: E712
            if market.series_id:
                continue
            series_slug = _match_series_slug(
                market.slug,
                self._series_slugs_sorted,
                self._series_by_slug,
            )
            if not series_slug:
                continue
            series_id = self._series_by_slug.get(series_slug)
            if not series_id:
                continue
            market.series_id = series_id
            self._session.add(market)
            updated += 1
        self._session.commit()
        return updated

    def _sync_tags_from_events(self) -> int:
        count = 0
        events = {e.id: e for e in self._session.exec(select(Event)).all()}
        for market in self._session.exec(
            select(Market).where(Market.event_id != None)  # noqa: E711
        ).all():
            if not market.event_id:
                continue
            event = events.get(market.event_id)
            if not event:
                continue
            tags = list(event.tags_json) if event.tags_json else []
            if not tags:
                tags = _tags_from_event_raw(event.raw_payload)
            for tag in tags:
                tag_l = tag.lower().strip()
                if not tag_l:
                    continue
                exists = self._session.exec(
                    select(MarketTag).where(
                        MarketTag.condition_id == market.condition_id,
                        MarketTag.tag == tag_l,
                    )
                ).first()
                if exists:
                    continue
                self._session.add(MarketTag(condition_id=market.condition_id, tag=tag_l))
                count += 1
        self._session.commit()
        return count

    def _apply_domain_from_events(self) -> int:
        updated = 0
        events = {e.id: e for e in self._session.exec(select(Event)).all()}
        for market in self._session.exec(
            select(Market).where(Market.event_id != None)  # noqa: E711
        ).all():
            if not market.event_id or market.domain not in ("unknown", ""):
                continue
            event = events.get(market.event_id)
            if not event:
                continue
            tags = list(event.tags_json) if event.tags_json else _tags_from_event_raw(
                event.raw_payload
            )
            domain = classify_domain_from_tags(tags)
            if domain == "unknown":
                domain = classify_domain_from_tags(_tags_from_event_raw(event.raw_payload))
            if domain != "unknown":
                market.domain = domain
                self._session.add(market)
                updated += 1
        self._session.commit()
        return updated

    def _backfill_series_from_events(self) -> int:
        result = self._session.execute(
            text(
                """
                UPDATE markets
                SET series_id = (
                    SELECT e.series_id FROM events e
                    WHERE e.id = markets.event_id
                      AND e.series_id IS NOT NULL AND e.series_id != ''
                )
                WHERE (series_id IS NULL OR series_id = '')
                  AND event_id IS NOT NULL AND event_id != ''
                  AND EXISTS (
                    SELECT 1 FROM events e
                    WHERE e.id = markets.event_id
                      AND e.series_id IS NOT NULL AND e.series_id != ''
                  )
                """
            )
        )
        self._session.commit()
        return int(result.rowcount or 0)  # type: ignore[attr-defined]


def _match_series_slug(
    market_slug: str,
    series_slugs: list[str],
    series_by_slug: dict[str, str],
) -> str | None:
    slug = market_slug.lower()
    for series_slug in series_slugs:
        if slug.startswith(series_slug + "-") or slug == series_slug:
            return series_slug
    for pattern, series_slug in _SLUG_PREFIX_RULES:
        if pattern.match(slug) and series_slug in series_by_slug:
            return series_slug
    return None


def _tags_from_event_raw(raw: dict[str, object]) -> list[str]:
    tags: list[str] = []
    raw_tags = raw.get("tags") if isinstance(raw, dict) else None
    if not isinstance(raw_tags, list):
        return tags
    for item in raw_tags:
        if isinstance(item, dict):
            slug = item.get("slug") or item.get("label")
            if slug:
                tags.append(str(slug).lower())
        elif isinstance(item, str):
            tags.append(item.lower())
    return tags
