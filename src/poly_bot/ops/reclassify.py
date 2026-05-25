"""Batch re-classification of markets from stored raw_payload."""

from __future__ import annotations

import structlog
from sqlalchemy import func
from sqlmodel import Session, select

from poly_bot.catalog.taxonomy import TaxonomyOverrideStore
from poly_bot.discovery.classifier.rules import classify_market
from poly_bot.discovery.fees import gamma_market_fee_fields
from poly_bot.persistence.schema import Market

logger = structlog.get_logger(__name__)


def reclassify_markets(
    session: Session,
    *,
    batch_size: int = 2000,
    only_unknown: bool = True,
    force: bool = False,
) -> dict[str, int]:
    """Re-run classifier rules on stored Gamma payloads."""
    updated = 0
    skipped = 0
    errors = 0
    offset = 0
    overrides = TaxonomyOverrideStore.load(session)

    while True:
        stmt = select(Market).limit(batch_size)
        if only_unknown and not force:
            stmt = stmt.where(Market.domain == "unknown")
        elif not force:
            stmt = stmt.offset(offset)
        rows = list(session.exec(stmt).all())
        if not rows:
            break

        for market in rows:
            raw = market.raw_payload
            if not isinstance(raw, dict) or len(raw) < 2:
                skipped += 1
                continue
            try:
                fee = gamma_market_fee_fields(raw)
                classification = classify_market(
                    raw,
                    slug=market.slug,
                    start_ts=market.start_ts,
                    end_ts=market.end_ts,
                    volume_24h=fee.get("volume_24h"),
                    liquidity_usd=fee.get("liquidity_usd"),
                    overrides=overrides,
                )
                market.structure = classification["structure"]
                market.domain = classification["domain"]
                market.horizon = classification["horizon"]
                market.recurrence = classification["recurrence"]
                market.resolution_source = classification["resolution_source"]
                market.liquidity_tier = classification["liquidity_tier"]
                session.add(market)
                updated += 1
            except Exception:
                errors += 1
                logger.exception("reclassify_failed", condition_id=market.condition_id)

        session.commit()
        if only_unknown and not force:
            break
        offset += batch_size

    logger.info("reclassify_done", updated=updated, skipped=skipped, errors=errors)
    return {"updated": updated, "skipped": skipped, "errors": errors}
