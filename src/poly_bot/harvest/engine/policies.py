"""Per-domain execution policies."""

from __future__ import annotations

from dataclasses import dataclass

from poly_bot.harvest.engine.state import ExecutionCandidate, MarketDuration, MonitoredMarket
from poly_bot.settings import Settings


@dataclass(frozen=True)
class PolicyVerdict:
    approved: bool
    reason: str  # always set — explains approval or rejection


class CryptoPolicy:
    """Guards specific to short-cycle crypto markets."""

    def max_entry_minutes(self, market: MonitoredMarket, settings: Settings) -> float:
        if market.duration == MarketDuration.MIN5:
            return settings.engine_max_entry_minutes_5m
        if market.duration == MarketDuration.MIN15:
            return settings.engine_max_entry_minutes_15m
        if market.duration == MarketDuration.HOUR1:
            return settings.engine_max_entry_minutes_60m
        # unknown duration → conservative
        return settings.engine_max_entry_minutes_5m

    def check_entry_timing(
        self, market: MonitoredMarket, settings: Settings
    ) -> PolicyVerdict:
        mins = market.minutes_left
        allowed = self.max_entry_minutes(market, settings)
        if mins > allowed:
            return PolicyVerdict(
                False,
                f"too_early:{mins:.1f}m_left>max_{allowed:.1f}m"
                f"(dur={market.duration.value}s)",
            )
        if mins <= 0:
            return PolicyVerdict(False, "expired")
        return PolicyVerdict(True, f"timing_ok:{mins:.1f}m_left")

    def check(
        self,
        candidate: ExecutionCandidate,
        open_by_underlying: dict[str, int],
        settings: Settings,
    ) -> PolicyVerdict:
        # timing gate
        t = self.check_entry_timing(candidate.market, settings)
        if not t.approved:
            return t

        # per-underlying concurrency limit
        underlying = candidate.market.underlying or "unknown"
        open_count = open_by_underlying.get(underlying, 0)
        if open_count >= settings.engine_max_positions_per_underlying:
            return PolicyVerdict(
                False,
                f"underlying_cap:{underlying}:{open_count}>="
                f"{settings.engine_max_positions_per_underlying}",
            )

        return PolicyVerdict(True, "crypto_ok")


class SportsPolicy:
    """Guards specific to sports markets."""

    def check(
        self,
        candidate: ExecutionCandidate,
        open_by_underlying: dict[str, int],
        settings: Settings,
    ) -> PolicyVerdict:
        market = candidate.market
        # sports: require in-play (is_live check happens upstream in scan)
        if market.minutes_left <= 0:
            return PolicyVerdict(False, "expired")
        # for sports we don't apply a tight time ceiling — book sanity is the guard
        return PolicyVerdict(True, "sports_ok")


def get_policy(domain: str) -> CryptoPolicy | SportsPolicy:
    if domain == "crypto":
        return CryptoPolicy()
    return SportsPolicy()
