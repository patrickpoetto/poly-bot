"""Reject harvest candidates with stale or crossed books."""

from __future__ import annotations

from poly_bot.settings import Settings


def max_bid_ask_spread_for_domain(settings: Settings, domain: str) -> float:
    """Per-domain spread cap (0 = disabled). Sports in-play books need a tighter cap."""
    if domain == "sports":
        return settings.harvest_sports_max_bid_ask_spread
    return settings.harvest_max_bid_ask_spread


def book_sanity_reject_reason(
    *,
    ask: float,
    bid: float | None,
    other_bid: float | None,
    max_bid_ask_spread: float,
    max_complement_sum: float,
) -> str | None:
    """Return reject reason, or None if the book looks consistent.

    Complement (ask + other_bid ≤ ~1) catches sports traps where a losing
    outcome shows a stale 98¢ ask while the other side bids 97¢.

    Spread is optional and domain-specific — weather books can be 15¢ wide
    but still sane when complement holds.
    """
    if max_bid_ask_spread > 0:
        if bid is None:
            return "no_bid"
        spread = ask - bid
        if spread > max_bid_ask_spread:
            return f"spread_{spread:.3f}"
    if other_bid is not None and max_complement_sum > 0:
        complement = ask + other_bid
        if complement > max_complement_sum:
            return f"complement_{complement:.3f}"
    return None
