"""Load and refresh CLOB asks for harvest scan."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlmodel import Session, col, select

from poly_bot.exchange.clob_public import ClobPublicClient
from poly_bot.harvest.liquidity import fillable_usd_at_ask, parse_book_levels
from poly_bot.persistence.schema import Token, TokenQuote
from poly_bot.settings import Settings


@dataclass(frozen=True, slots=True)
class SideQuote:
    token_id: str
    outcome_label: str
    mid: float | None
    ask: float | None
    quote_age_s: float | None
    fillable_usd: float = 0.0
    vwap_ask: float | None = None


async def refresh_side_quotes(
    session: Session,
    settings: Settings,
    *,
    condition_ids: list[str],
    order_usd: float,
    max_price: float,
) -> dict[str, dict[str, SideQuote]]:
    """Live CLOB /book for each outcome; returns {condition_id: {yes|no: SideQuote}}."""
    if not condition_ids:
        return {}

    tokens = list(
        session.exec(
            select(Token).where(col(Token.condition_id).in_(condition_ids))
        ).all()
    )
    by_cid: dict[str, dict[str, SideQuote]] = {}
    now = datetime.now(UTC)
    clob = ClobPublicClient(settings)
    sem = asyncio.Semaphore(settings.harvest_book_concurrency)

    async def one(token: Token) -> tuple[str, str, SideQuote]:
        side = "yes" if token.outcome_index == 0 else "no"
        db_q = session.get(TokenQuote, token.token_id)
        quote_age: float | None = None
        if db_q and db_q.updated_at:
            ts = db_q.updated_at
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            quote_age = max(0.0, (now - ts.astimezone(UTC)).total_seconds())

        async with sem:
            book = await clob.fetch_book(token.token_id)

        asks = parse_book_levels(book.get("asks") if isinstance(book, dict) else None)
        best_ask = asks[0][0] if asks else None
        if best_ask is None and db_q and db_q.ask is not None:
            best_ask = db_q.ask
        fillable, vwap = fillable_usd_at_ask(
            asks, order_usd=order_usd, max_price=max_price
        )
        if fillable <= 0 and best_ask is not None and best_ask <= max_price:
            fillable = order_usd
            vwap = best_ask

        mid = db_q.mid if db_q else None
        if mid is None and book and isinstance(book, dict):
            bids = parse_book_levels(book.get("bids"))
            if bids and asks:
                mid = (bids[-1][0] + asks[0][0]) / 2.0

        return token.condition_id, side, SideQuote(
            token_id=token.token_id,
            outcome_label=token.outcome_label,
            mid=mid,
            ask=best_ask,
            quote_age_s=quote_age,
            fillable_usd=fillable,
            vwap_ask=vwap,
        )

    results = await asyncio.gather(*(one(t) for t in tokens))
    for cid, side, sq in results:
        by_cid.setdefault(cid, {})[side] = sq
    return by_cid


def side_quotes_from_db(session: Session, condition_ids: list[str]) -> dict[str, dict[str, SideQuote]]:
    """Quotes already in token_quotes (no live book)."""
    if not condition_ids:
        return {}
    tokens = list(
        session.exec(
            select(Token).where(col(Token.condition_id).in_(condition_ids))
        ).all()
    )
    now = datetime.now(UTC)
    out: dict[str, dict[str, SideQuote]] = {}
    for token in tokens:
        side = "yes" if token.outcome_index == 0 else "no"
        db_q = session.get(TokenQuote, token.token_id)
        quote_age: float | None = None
        if db_q and db_q.updated_at:
            ts = db_q.updated_at
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            quote_age = max(0.0, (now - ts.astimezone(UTC)).total_seconds())
        sq = SideQuote(
            token_id=token.token_id,
            outcome_label=token.outcome_label,
            mid=db_q.mid if db_q else None,
            ask=db_q.ask if db_q else None,
            quote_age_s=quote_age,
        )
        out.setdefault(token.condition_id, {})[side] = sq
    return out
