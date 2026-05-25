"""Repository for catalog upserts and queries."""

from datetime import UTC, datetime

from sqlalchemy import func
from sqlmodel import Session, col, select

from poly_bot.persistence.schema import (
    CatalogRun,
    Event,
    Market,
    MarketEnrichment,
    MarketTag,
    Series,
    Token,
)


class CatalogRepository:
    """Persistence operations for the discovery engine."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert_market(self, market: Market) -> None:
        existing = self._session.get(Market, market.condition_id)
        now = datetime.now(UTC)
        if existing:
            for field in (
                "slug",
                "question",
                "event_id",
                "series_id",
                "structure",
                "domain",
                "horizon",
                "recurrence",
                "resolution_source",
                "liquidity_tier",
                "neg_risk",
                "neg_risk_market_id",
                "active",
                "closed",
                "accepting_orders",
                "enable_order_book",
                "start_ts",
                "end_ts",
                "tick_size",
                "min_order_size",
                "rewards_eligible",
                "fees_enabled",
                "fee_type",
                "maker_base_fee",
                "taker_base_fee",
                "fee_schedule",
                "raw_payload",
            ):
                setattr(existing, field, getattr(market, field))
            existing.last_seen_at = now
            self._session.add(existing)
        else:
            market.first_seen_at = now
            market.last_seen_at = now
            self._session.add(market)

    def upsert_series(self, series: Series) -> None:
        existing = self._session.get(Series, series.id)
        now = datetime.now(UTC)
        if existing:
            for field in ("slug", "title", "recurrence_seconds", "raw_payload"):
                setattr(existing, field, getattr(series, field))
            existing.last_seen_at = now
            self._session.add(existing)
        else:
            series.first_seen_at = now
            series.last_seen_at = now
            self._session.add(series)

    def upsert_event(self, event: Event) -> None:
        existing = self._session.get(Event, event.id)
        now = datetime.now(UTC)
        if existing:
            for field in (
                "slug",
                "title",
                "series_id",
                "start_ts",
                "end_ts",
                "active",
                "closed",
                "tags_json",
                "raw_payload",
            ):
                setattr(existing, field, getattr(event, field))
            existing.last_seen_at = now
            self._session.add(existing)
        else:
            event.first_seen_at = now
            event.last_seen_at = now
            self._session.add(event)

    def replace_market_tags(self, condition_id: str, tags: list[str]) -> None:
        for row in self._session.exec(
            select(MarketTag).where(MarketTag.condition_id == condition_id)
        ).all():
            self._session.delete(row)
        for tag in tags:
            self._session.add(MarketTag(condition_id=condition_id, tag=tag.lower().strip()))

    def replace_tokens(self, condition_id: str, tokens: list[Token]) -> None:
        for row in self._session.exec(
            select(Token).where(Token.condition_id == condition_id)
        ).all():
            self._session.delete(row)
        for token in tokens:
            self._session.add(token)

    def start_run(self, run_type: str) -> CatalogRun:
        run = CatalogRun(run_type=run_type)
        self._session.add(run)
        self._session.commit()
        self._session.refresh(run)
        return run

    def finish_run(
        self,
        run: CatalogRun,
        *,
        markets_upserted: int = 0,
        events_upserted: int = 0,
        series_upserted: int = 0,
        pages_fetched: int = 0,
        errors: int = 0,
        notes: str = "",
    ) -> None:
        run.finished_at = datetime.now(UTC)
        run.markets_upserted = markets_upserted
        run.events_upserted = events_upserted
        run.series_upserted = series_upserted
        run.pages_fetched = pages_fetched
        run.errors = errors
        run.notes = notes
        self._session.add(run)
        self._session.commit()

    def count_markets(self, *, active_only: bool = False) -> int:
        stmt = select(func.count()).select_from(Market)
        if active_only:
            stmt = stmt.where(col(Market.active).is_(True))
        return int(self._session.exec(stmt).one())

    def count_events(self, *, active_only: bool = False) -> int:
        stmt = select(func.count()).select_from(Event)
        if active_only:
            stmt = stmt.where(col(Event.active).is_(True))
        return int(self._session.exec(stmt).one())

    def count_series(self) -> int:
        return int(self._session.exec(select(func.count()).select_from(Series)).one())

    def count_markets_with_event(self) -> int:
        stmt = (
            select(func.count())
            .select_from(Market)
            .where(col(Market.event_id).is_not(None))
            .where(col(Market.event_id) != "")
        )
        return int(self._session.exec(stmt).one())

    def backfill_market_series_from_events(self) -> int:
        """Copy series_id from events onto markets that share event_id."""
        from sqlalchemy import text

        result = self._session.execute(
            text(
                """
                UPDATE markets
                SET series_id = (
                    SELECT e.series_id FROM events e
                    WHERE e.id = markets.event_id
                      AND e.series_id IS NOT NULL
                      AND e.series_id != ''
                )
                WHERE (series_id IS NULL OR series_id = '')
                  AND event_id IS NOT NULL
                  AND event_id != ''
                  AND EXISTS (
                    SELECT 1 FROM events e
                    WHERE e.id = markets.event_id
                      AND e.series_id IS NOT NULL
                      AND e.series_id != ''
                  )
                """
            )
        )
        self._session.commit()
        return int(result.rowcount or 0)  # type: ignore[attr-defined]

    def count_markets_with_fees(self) -> int:
        stmt = (
            select(func.count())
            .select_from(Market)
            .where(col(Market.fees_enabled).is_(True))
        )
        return int(self._session.exec(stmt).one())

    def count_events_with_fees(self) -> int:
        stmt = (
            select(func.count())
            .select_from(Event)
            .where(col(Event.fees_enabled).is_(True))
        )
        return int(self._session.exec(stmt).one())

    def count_markets_with_series(self) -> int:
        stmt = (
            select(func.count())
            .select_from(Market)
            .where(col(Market.series_id).is_not(None))
            .where(col(Market.series_id) != "")
        )
        return int(self._session.exec(stmt).one())

    def upsert_market_enrichment(
        self,
        condition_id: str,
        *,
        volume_24h: float | None = None,
        fee_rate_bps: int | None = None,
        maker_base_fee: int | None = None,
        taker_base_fee: int | None = None,
        rewards_min_size: float | None = None,
        rewards_max_spread: float | None = None,
    ) -> None:
        now = datetime.now(UTC)
        row = self._session.get(MarketEnrichment, condition_id)
        if row is None:
            row = MarketEnrichment(condition_id=condition_id)
        if volume_24h is not None:
            row.volume_24h = volume_24h
        if fee_rate_bps is not None:
            row.fee_rate_bps = fee_rate_bps
        if maker_base_fee is not None:
            row.maker_base_fee = maker_base_fee
        if taker_base_fee is not None:
            row.taker_base_fee = taker_base_fee
        if rewards_min_size is not None:
            row.rewards_min_size = rewards_min_size
        if rewards_max_spread is not None:
            row.rewards_max_spread = rewards_max_spread
        row.enriched_at = now
        self._session.add(row)

    def rollup_event_fees(self) -> int:
        """Aggregate fee fields from markets onto parent events."""
        from sqlalchemy import text

        result = self._session.execute(
            text(
                """
                UPDATE events
                SET fees_enabled = COALESCE((
                    SELECT MAX(CASE WHEN m.fees_enabled THEN 1 ELSE 0 END)
                    FROM markets m WHERE m.event_id = events.id
                ), 0),
                maker_base_fee = (
                    SELECT MIN(m.maker_base_fee) FROM markets m
                    WHERE m.event_id = events.id AND m.maker_base_fee IS NOT NULL
                ),
                taker_base_fee = (
                    SELECT MIN(m.taker_base_fee) FROM markets m
                    WHERE m.event_id = events.id AND m.taker_base_fee IS NOT NULL
                ),
                fee_type = (
                    SELECT m.fee_type FROM markets m
                    WHERE m.event_id = events.id AND m.fee_type IS NOT NULL
                    GROUP BY m.fee_type
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                ),
                fee_schedule = (
                    SELECT m.fee_schedule FROM markets m
                    WHERE m.event_id = events.id AND m.fee_schedule IS NOT NULL
                    LIMIT 1
                )
                WHERE EXISTS (
                    SELECT 1 FROM markets m WHERE m.event_id = events.id
                )
                """
            )
        )
        self._session.commit()
        return int(result.rowcount or 0)  # type: ignore[attr-defined]
