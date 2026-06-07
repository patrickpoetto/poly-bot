"""Backfill fee fields from stored Gamma raw_payload."""

from __future__ import annotations

import json

import structlog
from sqlalchemy import func
from sqlmodel import Session, select

from poly_bot.discovery.fees import gamma_market_fee_fields, resolve_fee_type
from poly_bot.discovery.mapper import gamma_market_enrichment_fields
from poly_bot.persistence.repo import CatalogRepository
from poly_bot.persistence.schema import Market

logger = structlog.get_logger(__name__)


def backfill_fees_from_raw(
    session: Session,
    *,
    batch_size: int = 1000,
    only_missing: bool = True,
) -> dict[str, int]:
    """Re-parse fee columns from markets.raw_payload."""
    repo = CatalogRepository(session)
    processed = 0
    skipped = 0
    errors = 0
    offset = 0

    while True:
        stmt = select(Market).limit(batch_size)
        if only_missing:
            stmt = stmt.where(Market.fee_type == None)  # noqa: E711
        else:
            stmt = stmt.offset(offset)
        rows = list(session.exec(stmt).all())
        if not rows:
            break

        batch_processed = 0
        for market in rows:
            raw = market.raw_payload
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError:
                    raw = None
            if not raw or not isinstance(raw, dict) or len(raw) < 2:
                market.fee_type = "none"
                market.fees_enabled = False
                session.add(market)
                skipped += 1
                batch_processed += 1
                continue

            try:
                fee = gamma_market_fee_fields(raw)
                market.fees_enabled = bool(fee.get("fees_enabled"))
                market.fee_type = resolve_fee_type(fee)
                market.maker_base_fee = fee.get("maker_base_fee")
                market.taker_base_fee = fee.get("taker_base_fee")
                market.fee_schedule = fee.get("fee_schedule")
                if fee.get("tick_size"):
                    market.tick_size = fee["tick_size"]
                if fee.get("min_order_size"):
                    market.min_order_size = fee["min_order_size"]
                market.rewards_eligible = bool(fee.get("rewards_eligible"))
                session.add(market)

                enrich = gamma_market_enrichment_fields(raw)
                repo.upsert_market_enrichment(
                    market.condition_id,
                    volume_24h=enrich.get("volume_24h"),
                    fee_rate_bps=enrich.get("fee_rate_bps"),
                    maker_base_fee=enrich.get("maker_base_fee"),
                    taker_base_fee=enrich.get("taker_base_fee"),
                    rewards_min_size=enrich.get("rewards_min_size"),
                    rewards_max_spread=enrich.get("rewards_max_spread"),
                )
                batch_processed += 1
            except Exception:
                errors += 1
                market.fee_type = "error"
                session.add(market)
                batch_processed += 1
                logger.exception("backfill_fee_failed", condition_id=market.condition_id)

        session.commit()
        processed += batch_processed
        remaining_n = 0
        if only_missing:
            remaining_n = int(
                session.exec(
                    select(func.count()).select_from(Market).where(Market.fee_type == None)  # noqa: E711
                ).one()
            )
        offset += batch_size if not only_missing else 0
        logger.info(
            "backfill_fees_progress",
            batch_processed=batch_processed,
            total_processed=processed,
            remaining=remaining_n,
        )
        if only_missing and batch_processed == 0:
            break

    events_rolled = repo.rollup_event_fees()
    logger.info(
        "backfill_fees_done",
        processed=processed,
        skipped=skipped,
        errors=errors,
        events_rolled=events_rolled,
    )
    return {
        "processed": processed,
        "skipped": skipped,
        "errors": errors,
        "events_rolled": events_rolled,
    }
