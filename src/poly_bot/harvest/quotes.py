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


def _book_age_seconds(book: dict[str, object], now: datetime) -> float | None:
    raw = book.get("timestamp")
    if raw is None:
        return None
    try:
        ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return max(0.0, (now - ts.astimezone(UTC)).total_seconds())


@dataclass(frozen=True, slots=True)
class SideQuote:
    token_id: str
    outcome_label: str
    mid: float | None
    bid: float | None
    ask: float | None
    quote_age_s: float | None
    book_age_s: float | None = None
    book_fetched: bool = False
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
        session.exec(select(Token).where(col(Token.condition_id).in_(condition_ids))).all()
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

        book_fetched = isinstance(book, dict)
        book_age_s: float | None = None
        bids: list[tuple[float, float]] = []
        asks: list[tuple[float, float]] = []
        if book_fetched and book is not None:
            book_age_s = _book_age_seconds(book, now)
            bids = parse_book_levels(book.get("bids"))
            asks = parse_book_levels(book.get("asks"))
        best_bid = bids[-1][0] if bids else None
        best_ask = asks[0][0] if asks else None
        if not book_fetched:
            if best_bid is None and db_q and db_q.bid is not None:
                best_bid = db_q.bid
            if best_ask is None and db_q and db_q.ask is not None:
                best_ask = db_q.ask
            if best_ask is None and db_q and db_q.mid is not None and db_q.mid >= 0.90:
                best_ask = db_q.mid
        fillable, vwap = fillable_usd_at_ask(asks, order_usd=order_usd, max_price=max_price)

        mid = db_q.mid if db_q else None
        if mid is None and best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2.0

        return (
            token.condition_id,
            side,
            SideQuote(
                token_id=token.token_id,
                outcome_label=token.outcome_label,
                mid=mid,
                bid=best_bid,
                ask=best_ask,
                quote_age_s=quote_age,
                book_age_s=book_age_s,
                book_fetched=book_fetched,
                fillable_usd=fillable,
                vwap_ask=vwap,
            ),
        )

    results = await asyncio.gather(*(one(t) for t in tokens))
    for cid, side, sq in results:
        by_cid.setdefault(cid, {})[side] = sq
    return by_cid


def side_quotes_from_db(
    session: Session, condition_ids: list[str]
) -> dict[str, dict[str, SideQuote]]:
    """Quotes already in token_quotes (no live book)."""
    if not condition_ids:
        return {}
    tokens = list(
        session.exec(select(Token).where(col(Token.condition_id).in_(condition_ids))).all()
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
            bid=db_q.bid if db_q else None,
            ask=db_q.ask if db_q else None,
            quote_age_s=quote_age,
        )
        out.setdefault(token.condition_id, {})[side] = sq
    return out
