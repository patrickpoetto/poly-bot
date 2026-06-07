"""HOT-tier time rules for catalog quotes and harvest."""

from __future__ import annotations

from poly_bot.catalog.models import ExpiringMarket
from poly_bot.settings import Settings


def in_hot_window(m: ExpiringMarket, settings: Settings) -> bool:
    """Whether a catalog market is in the HOT tier.

    Non-sports: ``hours_left`` until ``window_end`` ≤ ``catalog_quote_hot_hours``.

    Sports in-play: HOT from kickoff through ``window_end`` (entire live window).
    Sports pre-match: HOT only when kickoff is within ``catalog_quote_hot_hours``.
    """
    if m.hours_left <= 0:
        return False
    if m.domain == "sports":
        if m.is_live:
            return True
        if m.hours_to_kickoff is not None:
            return m.hours_to_kickoff <= settings.catalog_quote_hot_hours
        return m.hours_left <= settings.catalog_quote_hot_hours
    return m.hours_left <= settings.catalog_quote_hot_hours


def hot_window_label(settings: Settings) -> str:
    hot = settings.catalog_quote_hot_hours
    return f"HOT ≤{hot:.0f}h (sports: from kickoff)"
