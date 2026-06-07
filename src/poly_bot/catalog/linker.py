"""Link markets to series and tags using events + slug patterns.

Memory-efficient: streams rows from Postgres/SQLite, bulk-inserts with
ON CONFLICT/OR IGNORE, batched UPDATEs by target domain. Avoids loading
all events / markets into Python at once.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any

import structlog
from sqlalchemy import text
from sqlmodel import Session, select

from poly_bot.discovery.classifier.rules import classify_domain_from_tags
from poly_bot.persistence.schema import Series

logger = structlog.get_logger(__name__)

_BATCH = 5000

_SLUG_PREFIX_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^btc-updown-5m-"), "btc-updown-5m"),
    (re.compile(r"^btc-updown-15m-"), "btc-updown-15m"),
    (re.compile(r"^eth-updown-5m-"), "eth-updown-5m"),
    (re.compile(r"^eth-updown-15m-"), "eth-updown-15m"),
]


class CatalogLinker:
    def __init__(self, session: Session) -> None:
        self._session = session
        series_rows = list(self._session.exec(select(Series)).all())
        self._series_by_slug: dict[str, str] = {s.slug: s.id for s in series_rows if s.slug}
        self._series_slugs_sorted: list[str] = sorted(
            self._series_by_slug.keys(),
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

    # ── series linking by slug ───────────────────────────────────────────────

    def _link_series_by_slug(self) -> int:
        """Paginate active markets without series_id; batch UPDATEs grouped by series."""
        select_sql = text(
            """
            SELECT condition_id, slug
            FROM markets
            WHERE active = TRUE
              AND (series_id IS NULL OR series_id = '')
              AND condition_id > :last
            ORDER BY condition_id
            LIMIT :lim
            """
        )
        by_series: dict[str, list[str]] = defaultdict(list)
        last_cid = ""
        while True:
            rows = self._session.execute(select_sql, {"last": last_cid, "lim": _BATCH}).fetchall()
            if not rows:
                break
            for cid, slug in rows:
                series_slug = _match_series_slug(
                    str(slug or ""),
                    self._series_slugs_sorted,
                    self._series_by_slug,
                )
                if not series_slug:
                    continue
                series_id = self._series_by_slug.get(series_slug)
                if not series_id:
                    continue
                by_series[series_id].append(str(cid))
            last_cid = str(rows[-1][0])

        updated = 0
        for series_id, cids in by_series.items():
            for chunk in _chunks(cids, 500):
                placeholders = ",".join(f":c{i}" for i in range(len(chunk)))
                params: dict[str, str] = {f"c{i}": c for i, c in enumerate(chunk)}
                params["sid"] = series_id
                result = self._session.execute(
                    text(
                        f"UPDATE markets SET series_id = :sid "
                        f"WHERE condition_id IN ({placeholders})"
                    ),
                    params,
                )
                updated += int(result.rowcount or 0)  # type: ignore[attr-defined]
            self._session.commit()
        return updated

    # ── tags sync from events ────────────────────────────────────────────────

    def _sync_tags_from_events(self) -> int:
        """Insert (condition_id, tag) pairs from event JSON, bypassing Python memory.

        Postgres: single ``INSERT ... SELECT`` using ``jsonb_array_elements``.
        SQLite: paginated Python fallback (only ``tags_json`` loaded, no raw_payload).
        """
        if self._is_postgres():
            return self._sync_tags_pg()
        return self._sync_tags_sqlite()

    def _sync_tags_pg(self) -> int:
        sql = text(
            """
            INSERT INTO market_tags (condition_id, tag)
            SELECT cid, tag FROM (
                SELECT m.condition_id AS cid,
                       LOWER(TRIM(BOTH FROM
                           CASE jsonb_typeof(t)
                               WHEN 'string' THEN t #>> '{}'
                               WHEN 'object' THEN COALESCE(t->>'slug', t->>'label')
                               ELSE NULL
                           END)) AS tag
                FROM markets m
                JOIN events e ON e.id = m.event_id
                CROSS JOIN LATERAL jsonb_array_elements(
                    CASE
                        WHEN e.tags_json IS NOT NULL
                             AND jsonb_typeof(e.tags_json::jsonb) = 'array'
                        THEN e.tags_json::jsonb
                        ELSE '[]'::jsonb
                    END
                ) AS t
                WHERE m.event_id IS NOT NULL AND m.event_id <> ''
            ) src
            WHERE tag IS NOT NULL AND tag <> ''
            ON CONFLICT (condition_id, tag) DO NOTHING
            """
        )
        result = self._session.execute(sql)
        self._session.commit()
        return int(result.rowcount or 0)  # type: ignore[attr-defined]

    def _sync_tags_sqlite(self) -> int:
        insert_sql = "INSERT OR IGNORE INTO market_tags (condition_id, tag) VALUES (:cid, :tag)"
        select_sql = text(
            """
            SELECT m.condition_id, e.tags_json
            FROM markets m
            JOIN events e ON e.id = m.event_id
            WHERE m.event_id IS NOT NULL AND m.event_id != ''
              AND m.condition_id > :last
            ORDER BY m.condition_id
            LIMIT :lim
            """
        )
        inserted = 0
        last_cid = ""
        while True:
            rows = self._session.execute(select_sql, {"last": last_cid, "lim": _BATCH}).fetchall()
            if not rows:
                break
            batch: list[dict[str, str]] = []
            for cid, tags_json in rows:
                for tag in _parse_json_list(tags_json):
                    label: str | None = None
                    if isinstance(tag, str):
                        label = tag
                    elif isinstance(tag, dict):
                        raw = tag.get("slug") or tag.get("label")
                        label = str(raw) if raw else None
                    if not label:
                        continue
                    label = label.lower().strip()
                    if not label:
                        continue
                    batch.append({"cid": str(cid), "tag": label})
            if batch:
                result = self._session.execute(text(insert_sql), batch)
                inserted += int(result.rowcount or 0)  # type: ignore[attr-defined]
                self._session.commit()
            last_cid = str(rows[-1][0])
        return inserted

    # ── domain backfill ──────────────────────────────────────────────────────

    def _apply_domain_from_events(self) -> int:
        """Paginate unknown-domain markets; classify from already-materialized market_tags.

        Reads only ``(condition_id, tag)`` rows from ``market_tags`` — no event
        payloads — so memory stays flat regardless of catalog size.
        """
        select_sql = (
            text(
                """
            SELECT m.condition_id, COALESCE(
                (SELECT string_agg(mt.tag, ',') FROM market_tags mt
                 WHERE mt.condition_id = m.condition_id), ''
            ) AS tags
            FROM markets m
            WHERE (m.domain IS NULL OR m.domain = '' OR m.domain = 'unknown')
              AND m.condition_id > :last
            ORDER BY m.condition_id
            LIMIT :lim
            """
            )
            if self._is_postgres()
            else text(
                """
            SELECT m.condition_id, COALESCE(
                (SELECT GROUP_CONCAT(mt.tag, ',') FROM market_tags mt
                 WHERE mt.condition_id = m.condition_id), ''
            ) AS tags
            FROM markets m
            WHERE (m.domain IS NULL OR m.domain = '' OR m.domain = 'unknown')
              AND m.condition_id > :last
            ORDER BY m.condition_id
            LIMIT :lim
            """
            )
        )
        by_domain: dict[str, list[str]] = defaultdict(list)
        last_cid = ""
        while True:
            rows = self._session.execute(select_sql, {"last": last_cid, "lim": _BATCH}).fetchall()
            if not rows:
                break
            for cid, tags_csv in rows:
                if not tags_csv:
                    continue
                tags = [t for t in str(tags_csv).split(",") if t]
                domain = classify_domain_from_tags(tags)
                if domain == "unknown":
                    continue
                by_domain[domain].append(str(cid))
            last_cid = str(rows[-1][0])

        updated = 0
        for domain, cids in by_domain.items():
            for chunk in _chunks(cids, 500):
                placeholders = ",".join(f":c{i}" for i in range(len(chunk)))
                params: dict[str, str] = {f"c{i}": c for i, c in enumerate(chunk)}
                params["d"] = domain
                result = self._session.execute(
                    text(f"UPDATE markets SET domain = :d WHERE condition_id IN ({placeholders})"),
                    params,
                )
                updated += int(result.rowcount or 0)  # type: ignore[attr-defined]
            self._session.commit()
        return updated

    # ── series_id from events ────────────────────────────────────────────────

    def _backfill_series_from_events(self) -> int:
        is_pg = self._is_postgres()
        if is_pg:
            sql = text(
                """
                UPDATE markets
                SET series_id = e.series_id
                FROM events e
                WHERE markets.event_id = e.id
                  AND (markets.series_id IS NULL OR markets.series_id = '')
                  AND e.series_id IS NOT NULL AND e.series_id != ''
                """
            )
        else:
            sql = text(
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
        result = self._session.execute(sql)
        self._session.commit()
        return int(result.rowcount or 0)  # type: ignore[attr-defined]

    def _is_postgres(self) -> bool:
        bind = self._session.get_bind()
        return bind is not None and bind.dialect.name == "postgresql"


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


def _normalize_tags(tags_json: object, raw_payload: object) -> list[str]:
    """Extract a list of tag slugs from a JSON column + raw event payload."""
    tags = _parse_json_list(tags_json)
    result: list[str] = []
    for item in tags:
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict):
            slug = item.get("slug") or item.get("label")
            if slug:
                result.append(str(slug))
    if result:
        return result
    raw = _parse_json_dict(raw_payload)
    if raw is None:
        return result
    inner = raw.get("tags") if isinstance(raw, dict) else None
    if isinstance(inner, list):
        for item in inner:
            if isinstance(item, dict):
                slug = item.get("slug") or item.get("label")
                if slug:
                    result.append(str(slug))
            elif isinstance(item, str):
                result.append(item)
    return result


def _parse_json_list(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _parse_json_dict(value: object) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]
