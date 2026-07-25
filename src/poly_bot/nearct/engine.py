"""NearCT engine — sequential near-certainty compounding strategy.

Loop:
  1. Scan CLOB for markets expiring in the next `scan_horizon_min` minutes
     where ask >= ask_min.
  2. Pick the best candidate (highest ask, deepest book).
  3. Place a paper order for the current capital.
  4. Wait for market resolution (polling every 30 s via DB).
  5. On WIN: capital *= (1 + net_yield_pct).
     On LOSS: log it, reset capital to start_capital.
  6. Repeat.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger(__name__)

_SCAN_INTERVAL_S = 20.0
_QUOTE_POLL_S = 5.0  # fill/risk check cadence while paper bids rest
_MAX_PARALLEL_QUOTES = 10  # markets quoted at once; first fill claims the capital
_QUOTE_REFRESH_S = 60.0  # rebuild the quote set this often (fresh flow/sigma)
_RESOLUTION_POLL_S = 10.0  # onchain eth_call is ~100ms; 10s polling is cheap
_RESOLUTION_GRACE_S = 20.0  # crypto 5m/15m land onchain ~60-90s after end_ts
_REAP_EVERY_N_SCANS = 30  # re-check stuck orders roughly every 10 min of scanning

# Max minutes to wait for resolution after end_ts before giving up (by domain).
# With onchain detection these are pure safety nets — crypto resolves in ~90s.
_MAX_HOLD_AFTER_END_MIN: dict[str, float] = {
    "crypto": 45.0,  # 5m resolves ~90s but DAILY updown takes 10-30 min — 45 covers both
    "sports": 150.0,  # BO3/long matches overrun the 135-min whistle proxy; UMA adds 20-40 min
    "culture": 60.0,
    "politics": 120.0,
    "finance": 60.0,
    "weather": 15.0,
    "unknown": 15.0,
}


@dataclass
class NearctStatus:
    """Exposed to the dashboard API."""

    state: str = "idle"  # idle | scanning | quoting | holding | stopped
    capital: float = 0.0
    start_capital: float = 0.0
    open_order: dict[str, Any] | None = None  # None while scanning
    open_quote: dict[str, Any] | None = None  # resting paper bid (maker mode)
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    stuck: int = 0  # timed-out unresolved positions (capital reset)
    trades_today: int = 0
    total_pnl: float = 0.0
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class ScanEvent:
    ts: datetime
    markets_in_window: int
    mid_passed: int
    book_checked: int
    candidates: int
    best_ask: float | None = None
    best_yield_pct: float | None = None
    best_minutes_left: float | None = None
    action: str = "no_markets"  # no_markets | filtered_out | placed | error
    rejected: list[dict[str, Any]] = field(default_factory=list)


class NearctEngine:
    def __init__(self, settings: object) -> None:
        from poly_bot.nearct.resolution import OnchainResolver
        from poly_bot.settings import Settings

        self._settings: Settings = settings  # type: ignore[assignment]
        self._status = NearctStatus(
            start_capital=self._settings.nearct_start_capital,
            capital=self._settings.nearct_start_capital,
        )
        from poly_bot.nearct.maker import PriceFeed

        self._scan_log: deque[ScanEvent] = deque(maxlen=50)
        self._generation: int = 0
        self._resolver = OnchainResolver(rpc_url=self._settings.rpc_url)
        self._scans_since_reap: int = 0
        self._feed = PriceFeed()

    @property
    def status(self) -> NearctStatus:
        return self._status

    def reset(self) -> None:
        """Delete all nearct_orders and reset in-memory state. Run in thread pool."""
        from sqlalchemy import delete

        from poly_bot.nearct.schema import NearctOrder, get_nearct_engine

        engine = get_nearct_engine(self._settings)
        with engine.connect() as conn:
            conn.execute(delete(NearctOrder))
            conn.commit()

        self._status.capital = self._settings.nearct_start_capital
        self._status.start_capital = self._settings.nearct_start_capital
        self._status.total_trades = 0
        self._status.wins = 0
        self._status.losses = 0
        self._status.stuck = 0
        self._status.trades_today = 0
        self._status.total_pnl = 0.0
        self._status.open_order = None
        self._status.state = "idle"
        self._scan_log.clear()
        self._generation += 1
        log.info(
            "nearct_reset: session cleared, capital reset to %.2f",
            self._settings.nearct_start_capital,
        )

    @property
    def scan_log(self) -> list[dict[str, Any]]:
        return [
            {
                "ts": e.ts.isoformat(),
                "markets_in_window": e.markets_in_window,
                "mid_passed": e.mid_passed,
                "book_checked": e.book_checked,
                "candidates": e.candidates,
                "best_ask": e.best_ask,
                "best_yield_pct": e.best_yield_pct,
                "best_minutes_left": (
                    round(e.best_minutes_left, 1) if e.best_minutes_left is not None else None
                ),
                "action": e.action,
                "rejected": e.rejected,
            }
            for e in self._scan_log
        ]

    async def run(self) -> None:
        log.info(
            "nearct_engine_start(v3-maker) capital=%.2f min_sigma=%.1f target_yield=%.2f%%",
            self._settings.nearct_start_capital,
            self._settings.nearct_min_sigma,
            self._settings.nearct_target_yield_pct * 100,
        )

        loop = asyncio.get_running_loop()

        # Restore capital + session stats from DB (survive dashboard restarts)
        capital = await loop.run_in_executor(None, self._load_capital)
        self._status.capital = capital
        self._status.start_capital = self._settings.nearct_start_capital
        await loop.run_in_executor(None, self._load_session_stats)

        # Retry stuck orders from prior runs — resolve late for reporting
        await loop.run_in_executor(None, self._reap_stuck)

        # Resume any pending order from a prior run
        pending = await loop.run_in_executor(None, self._load_pending_order)
        if pending is not None:
            log.info("nearct_engine: resuming pending order %s", pending["id"][:8])
            await self._hold_and_resolve(pending, capital)
            return  # loop resumes from top after resolution

        while True:
            self._status.state = "scanning"
            self._status.open_quote = None
            self._status.last_updated = datetime.now(UTC)
            # status.capital is the source of truth: resolutions update it and
            # a session reset rewrites it — a loop-local copy goes stale.
            capital = self._status.capital

            self._scans_since_reap += 1
            if self._scans_since_reap >= _REAP_EVERY_N_SCANS:
                self._scans_since_reap = 0
                await loop.run_in_executor(None, self._reap_stuck)

            filled = await self._quote_cycle(capital)
            if filled:
                capital = self._status.capital
            else:
                # Short nap after an active quoting round (set refresh) —
                # long naps would blind us to the 5m-window finales.
                nap = 3.0 if self._status.state == "quoting" else _SCAN_INTERVAL_S
                await asyncio.sleep(nap)

    async def _quote_cycle(self, capital: float) -> bool:
        """One maker cycle: pick the most certain market, rest a paper bid,
        watch real sell prints for fills, hold to resolution when filled.

        Returns True if a position was opened and resolved (capital updated).
        """
        import httpx

        from poly_bot.nearct.maker import check_fills, scan_quote_candidates

        gen = self._generation
        loop = asyncio.get_running_loop()

        try:
            candidates, funnel = await scan_quote_candidates(
                settings=self._settings, feed=self._feed
            )
        except Exception as exc:
            log.warning("nearct_quote_scan_error: %s", exc)
            self._scan_log.appendleft(
                ScanEvent(
                    ts=datetime.now(UTC),
                    markets_in_window=0,
                    mid_passed=0,
                    book_checked=0,
                    candidates=0,
                    action="error",
                )
            )
            return False

        # Merge in-play sports candidates (consensus-verified, 7%+ yields).
        # Sorted after crypto here, then re-ranked: highest yield first among
        # flow-bearing candidates — sports beats crypto when a match is live.
        if self._settings.nearct_sports_enabled:
            try:
                from poly_bot.nearct.sports import scan_sports_candidates

                sports_cands, sports_funnel = await scan_sports_candidates(settings=self._settings)
                # Guaranteed sports representation: sports yield is ~3-4x
                # crypto, so reserve up to half the quote slots for it
                # instead of letting crypto flow-sort them out.
                top_sports: list[Any] = list(sports_cands)[: _MAX_PARALLEL_QUOTES // 2]
                # No global re-sort: both lists arrive pre-sorted and a flow
                # sort would let crypto crowd sports back out of the slots.
                merged: list[Any] = top_sports + list(candidates)
                candidates = merged
                funnel["markets"] += sports_funnel["markets"]
                funnel["quoted"] += sports_funnel["quoted"]
            except Exception as exc:
                log.warning("nearct_sports_scan_error: %s", exc)

        event = ScanEvent(
            ts=datetime.now(UTC),
            markets_in_window=funnel["markets"],
            mid_passed=funnel["sigma_pass"],
            book_checked=funnel["quoted"],
            candidates=len(candidates),
            best_ask=candidates[0].bid_price if candidates else None,
            best_yield_pct=candidates[0].net_yield_pct if candidates else None,
            best_minutes_left=candidates[0].minutes_left if candidates else None,
            action="no_markets" if not candidates else "quoting",
        )

        if not candidates:
            self._scan_log.appendleft(event)
            return False

        # Taker shortcut: an ask already at/inside our yield target — grab it.
        take_c = next((x for x in candidates if x.take_ask and x.best_ask is not None), None)
        if take_c is not None:
            take_ask: float = take_c.best_ask  # type: ignore[assignment]
            if capital >= take_c.order_min_size * take_ask:
                if not await self._verify_accepting(take_c.condition_id):
                    event.action = "not_accepting"
                    self._scan_log.appendleft(event)
                    return False
                fee_rate = 0.05 if take_c.domain == "sports" else 0.07
                fee = fee_rate * (1 - take_ask)
                take_yield = (1 - take_ask - fee) / (take_ask + fee)
                event.action = "placed"
                self._scan_log.appendleft(event)
                log.info(
                    "nearct_take: %s ask=%.3f nsig=%.1f",
                    take_c.question[:40],
                    take_ask,
                    take_c.n_sigma,
                )
                order = await loop.run_in_executor(
                    None,
                    self._place_paper_fill,
                    take_c,
                    take_ask,
                    take_yield,
                    capital,
                    capital,
                )
                await self._hold_and_resolve(order, capital)
                return True

        # ── Rest paper bids on the top-N markets; first print wins ────────
        # One resting bid covers one market, but sell bursts scatter across
        # the whole updown family (measured: ~250 capturable prints / 4h).
        # Quote several at once with the same capital; the first fill claims
        # it and the other quotes are cancelled (paper: simply dropped).
        quotes = [x for x in candidates if capital >= x.order_min_size * x.bid_price][
            :_MAX_PARALLEL_QUOTES
        ]
        if not quotes:
            event.action = "below_min_size"
            self._scan_log.appendleft(event)
            return False

        c = quotes[0]
        self._status.state = "quoting"
        self._status.open_quote = {
            "question": c.question,
            "bid": c.bid_price,
            "net_yield_pct": c.net_yield_pct,
            "n_sigma": round(c.n_sigma, 1),
            "recent_trades": c.recent_trades,
            "minutes_left": round(c.minutes_left, 1),
            "end_ts": c.end_ts.isoformat(),
            "active_quotes": len(quotes),
        }
        self._status.last_updated = datetime.now(UTC)
        self._scan_log.appendleft(event)
        for q in quotes:
            log.info(
                "nearct_quote: %s bid=%.3f yield=%.2f%% nsig=%.1f mins=%.0f flow=%d",
                q.question[:45],
                q.bid_price,
                q.net_yield_pct * 100,
                q.n_sigma,
                q.minutes_left,
                q.recent_trades,
            )

        min_quote_s = self._settings.nearct_min_quote_minutes * 60
        refresh_deadline = datetime.now(UTC).timestamp() + _QUOTE_REFRESH_S
        since: dict[str, float] = {q.condition_id: datetime.now(UTC).timestamp() for q in quotes}
        winner = None
        filled_shares = 0.0
        target_shares = 0.0

        async with httpx.AsyncClient(timeout=httpx.Timeout(8.0)) as client:
            # Phase A — hunt: watch all quoted markets for the first print.
            while winner is None:
                if self._generation != gen:
                    return False
                now_ts = datetime.now(UTC).timestamp()
                if now_ts >= refresh_deadline:
                    return False  # refresh the candidate set (outer loop rescans)

                quotes = [q for q in quotes if q.end_ts.timestamp() - min_quote_s > now_ts]
                if not quotes:
                    return False

                for q in quotes:
                    shares = await check_fills(
                        client,
                        condition_id=q.condition_id,
                        token_id=q.token_id,
                        bid_price=q.bid_price,
                        since_ts=since[q.condition_id],
                    )
                    since[q.condition_id] = now_ts
                    if shares > 0:
                        winner = q
                        target_shares = capital / q.bid_price
                        filled_shares = min(target_shares, shares)
                        log.info(
                            "nearct_fill: %s +%.2f shares (%.0f%%)",
                            q.question[:40],
                            shares,
                            filled_shares / target_shares * 100,
                        )
                        break

                if winner is None:
                    # Risk gate: drop quotes whose certainty decayed
                    kept = []
                    for q in quotes:
                        if await self._quote_still_safe(client, q, now_ts):
                            kept.append(q)
                    quotes = kept
                    if not quotes:
                        return False
                    await asyncio.sleep(_QUOTE_POLL_S)

            # Phase B — accumulate on the winner until target/deadline/decay.
            c = winner
            quote_deadline = c.end_ts.timestamp() - min_quote_s
            while filled_shares < target_shares:
                if self._generation != gen:
                    return False
                now_ts = datetime.now(UTC).timestamp()
                if now_ts >= quote_deadline:
                    break
                new_shares = await check_fills(
                    client,
                    condition_id=c.condition_id,
                    token_id=c.token_id,
                    bid_price=c.bid_price,
                    since_ts=since[c.condition_id],
                )
                since[c.condition_id] = now_ts
                if new_shares > 0:
                    filled_shares = min(target_shares, filled_shares + new_shares)
                    log.info(
                        "nearct_fill: %s +%.2f shares (%.0f%%)",
                        c.question[:40],
                        new_shares,
                        filled_shares / target_shares * 100,
                    )
                if not await self._quote_still_safe(client, c, now_ts):
                    log.warning("nearct_quote_cancel(fill): risk break — holding partial")
                    break
                await asyncio.sleep(_QUOTE_POLL_S)

        filled_usd = round(filled_shares * c.bid_price, 4)
        self._log_quote_end("quote_filled", c)
        order = await loop.run_in_executor(
            None,
            self._place_paper_fill,
            c,
            c.bid_price,
            c.net_yield_pct,
            filled_usd,
            capital,
        )
        await self._hold_and_resolve(order, capital)
        return True

    async def _quote_still_safe(self, client: object, q: object, now_ts: float) -> bool:
        """Domain-aware dead-man switch for a resting quote.

        crypto: spot-to-strike sigma must stay above half the entry gate.
        sports: the live trade consensus must hold (book = fastest feed).
        """
        from poly_bot.nearct.maker import sigma_distance

        if q.domain == "sports":  # type: ignore[attr-defined]
            from poly_bot.nearct.sports import consensus_still_valid

            ok = await consensus_still_valid(client, self._settings, q)  # type: ignore[arg-type]
            if not ok:
                log.warning(
                    "nearct_quote_cancel: consensus broke on %s",
                    q.question[:40],  # type: ignore[attr-defined]
                )
            return ok

        mins = (q.end_ts.timestamp() - now_ts) / 60  # type: ignore[attr-defined]
        sd = await sigma_distance(
            client,  # type: ignore[arg-type]
            self._feed,
            question=q.question,  # type: ignore[attr-defined]
            outcome_label=q.outcome_label,  # type: ignore[attr-defined]
            event_start_iso=q.event_start_iso,  # type: ignore[attr-defined]
            minutes_left=mins,
        )
        if sd is not None and sd[1] < self._settings.nearct_min_sigma * 0.5:
            log.warning(
                "nearct_quote_cancel: sigma %.1f on %s",
                sd[1],
                q.question[:40],  # type: ignore[attr-defined]
            )
            return False
        return True

    def _log_quote_end(self, action: str, c: object) -> None:
        self._scan_log.appendleft(
            ScanEvent(
                ts=datetime.now(UTC),
                markets_in_window=0,
                mid_passed=0,
                book_checked=0,
                candidates=1,
                best_ask=c.bid_price,  # type: ignore[attr-defined]
                best_yield_pct=c.net_yield_pct,  # type: ignore[attr-defined]
                best_minutes_left=c.minutes_left,  # type: ignore[attr-defined]
                action=action,
            )
        )

    async def _verify_accepting(self, condition_id: str) -> bool:
        """Check CLOB accepting_orders for the chosen market. Fail-open on errors:
        the book was fetched seconds ago, a transient API error shouldn't stall us."""
        import httpx

        url = f"{self._settings.polymarket_clob_host}/markets/{condition_id}"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get(url)
                r.raise_for_status()
                accepting = r.json().get("accepting_orders")
                if accepting is False:
                    log.info("nearct: market %s not accepting orders — skip", condition_id[:12])
                    return False
                return True
        except Exception as exc:
            log.debug("nearct_verify_accepting_error (proceeding): %s", exc)
            return True

    async def _hold_and_resolve(self, order: dict[str, Any], capital_before: float) -> None:
        """Wait for the open order to resolve, then update capital and DB."""
        gen = self._generation
        self._status.state = "holding"
        raw_end_ts = order["end_ts"]
        end_ts_iso = raw_end_ts.isoformat() if isinstance(raw_end_ts, datetime) else str(raw_end_ts)
        self._status.open_order = {
            "id": order["id"],
            "condition_id": order["condition_id"],
            "question": order["question"],
            "ask": order["ask"],
            "net_yield_pct": order["net_yield_pct"],
            "size_usd": order["size_usd"],
            "end_ts": end_ts_iso,
        }
        self._status.last_updated = datetime.now(UTC)

        end_ts = _parse_dt(order["end_ts"])
        result = await self._wait_resolution(
            order["id"],
            order["condition_id"],
            order["token_id"],
            end_ts,
            gen,
            domain=order.get("domain", "unknown"),
        )
        self._status.open_order = None

        if result == "timeout":
            # Unresolved within the domain timeout. The position (and its capital)
            # is genuinely locked — a live account could NOT keep compounding with
            # it, so the honest model is: working capital resets to start_capital.
            # The stuck-order reaper will record the final outcome later.
            reset_capital = self._settings.nearct_start_capital
            await asyncio.get_running_loop().run_in_executor(
                None, self._resolve_order, order["id"], "stuck", 0.0, reset_capital
            )
            if self._generation == gen:
                self._status.capital = reset_capital
                self._status.stuck += 1
                self._status.state = "idle"
                self._status.last_updated = datetime.now(UTC)
                log.warning(
                    "nearct_stuck: $%.2f locked in %s — working capital reset to $%.2f",
                    order["size_usd"],
                    order["condition_id"][:12],
                    reset_capital,
                )
            return

        won = result == "won"
        if won:
            pnl = round(order["size_usd"] * order["net_yield_pct"], 4)
            capital_after = round(capital_before + pnl, 4)
        else:
            pnl = -round(order["size_usd"], 4)
            capital_after = self._settings.nearct_start_capital

        await asyncio.get_running_loop().run_in_executor(
            None, self._resolve_order, order["id"], result, pnl, capital_after
        )

        if self._generation != gen:
            log.info("nearct: session was reset during hold — discarding stale update")
            return

        self._status.capital = capital_after
        self._status.total_trades += 1
        self._status.trades_today += 1
        self._status.total_pnl = round(self._status.total_pnl + pnl, 4)
        if won:
            self._status.wins += 1
            log.info("nearct_won: +$%.4f  capital=$%.4f", pnl, capital_after)
        else:
            self._status.losses += 1
            log.warning(
                "nearct_lost: -$%.4f  capital reset to $%.2f",
                order["size_usd"],
                self._settings.nearct_start_capital,
            )
        self._status.last_updated = datetime.now(UTC)

    async def _wait_resolution(
        self,
        order_id: str,
        condition_id: str,
        token_id: str,
        end_ts: datetime,
        gen: int,
        domain: str = "unknown",
    ) -> str:
        """Wait for resolution; return 'won'/'lost'/'reset'/'timeout'.

        Primary source: Polygon CTF contract (payoutDenominator) — lands ~60-90s
        after end_ts for crypto, vs 30-50 min for the Gamma→DB path. The DB check
        stays as fallback for when every RPC is down.
        """
        from poly_bot.persistence.db import get_engine

        engine = get_engine(self._settings)
        loop = asyncio.get_running_loop()

        outcome_index = await loop.run_in_executor(
            None, _token_outcome_index_sync, engine, token_id
        )

        # Wait until end_ts + grace, waking every 5s to detect a session reset.
        now = datetime.now(UTC)
        check_after = end_ts.timestamp() + _RESOLUTION_GRACE_S
        wait_s = max(0.0, check_after - now.timestamp())
        if wait_s > 1:
            log.debug("nearct: waiting %.0fs for market to expire + grace", wait_s)
            deadline = loop.time() + wait_s
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                if self._generation != gen:
                    log.info("nearct: reset detected — aborting resolution wait")
                    return "reset"
                await asyncio.sleep(min(5.0, remaining))

        max_hold_min = _MAX_HOLD_AFTER_END_MIN.get(domain, 90.0)
        timeout_ts = end_ts.timestamp() + max_hold_min * 60
        db_fallback_every = 6  # check DB every ~60s in case all RPCs are down
        polls = 0

        while True:
            if self._generation != gen:
                log.info("nearct: reset detected — aborting resolution poll")
                return "reset"
            if datetime.now(UTC).timestamp() > timeout_ts:
                log.warning(
                    "nearct_timeout: order %s domain=%s unresolved after %.0f min — releasing loop",
                    order_id[:8],
                    domain,
                    max_hold_min,
                )
                return "timeout"

            if outcome_index is not None:
                try:
                    result = await self._resolver.check(condition_id, outcome_index)
                    if result is not None:
                        log.info("nearct_resolved_onchain: %s -> %s", condition_id[:12], result)
                        return result
                except Exception as exc:
                    log.debug("nearct_onchain_check_error: %s", exc)

            polls += 1
            if polls % db_fallback_every == 0 or outcome_index is None:
                try:
                    result = await loop.run_in_executor(
                        None, _check_resolution_sync, engine, condition_id, token_id
                    )
                    if result is not None:
                        log.info("nearct_resolved_db: %s -> %s", condition_id[:12], result)
                        return result
                except Exception as exc:
                    log.debug("nearct_resolution_check_error: %s", exc)

            await asyncio.sleep(_RESOLUTION_POLL_S)

    # ── DB helpers (sync, run in thread pool) ────────────────────────────

    def _load_capital(self) -> float:
        from sqlmodel import Session, select

        from poly_bot.nearct.schema import NearctOrder, get_nearct_engine

        engine = get_nearct_engine(self._settings)
        with Session(engine) as session:
            # Latest terminal order (won/lost/stuck/*_late) carries the current
            # working capital in capital_after: wins compound it, losses and
            # stuck orders reset it. Late reaper updates never touch it.
            last = session.exec(
                select(NearctOrder)
                .where(NearctOrder.status != "pending")
                .where(NearctOrder.capital_after.is_not(None))  # type: ignore[union-attr]
                .order_by(NearctOrder.created_at.desc())  # type: ignore[attr-defined]
            ).first()
            if last and last.capital_after is not None:
                log.info("nearct: resuming capital=%.4f from last order", last.capital_after)
                return last.capital_after
            return self._settings.nearct_start_capital

    def _load_pending_order(self) -> dict[str, Any] | None:
        from sqlmodel import Session, select

        from poly_bot.nearct.schema import NearctOrder, get_nearct_engine

        engine = get_nearct_engine(self._settings)
        with Session(engine) as session:
            pending = session.exec(
                select(NearctOrder)
                .where(NearctOrder.status == "pending")
                .order_by(NearctOrder.created_at.desc())  # type: ignore[attr-defined]
            ).first()
            if pending is None:
                return None
            return {
                "id": pending.id,
                "condition_id": pending.condition_id,
                "token_id": pending.token_id,
                "question": pending.question,
                "domain": pending.domain,
                "ask": pending.ask,
                "net_yield_pct": pending.net_yield_pct,
                "size_usd": pending.size_usd,
                "end_ts": pending.end_ts,
                "capital_before": pending.capital_before,
            }

    def _place_paper_fill(
        self,
        candidate: object,
        price: float,
        net_yield_pct: float,
        size_usd: float,
        capital_before: float,
    ) -> dict[str, Any]:
        """Persist a filled paper position (maker fill or taker take).

        size_usd may be less than capital_before on partial maker fills; the
        unfilled remainder stays in the working capital untouched.
        """
        from sqlmodel import Session

        from poly_bot.nearct.maker import QuoteCandidate
        from poly_bot.nearct.schema import NearctOrder, get_nearct_engine

        c: QuoteCandidate = candidate  # type: ignore[assignment]

        # Keep the id in a local: ORM attributes expire after commit and the
        # detached instance can't refresh once the session is closed.
        order_id = str(uuid.uuid4())
        order = NearctOrder(
            id=order_id,
            condition_id=c.condition_id,
            token_id=c.token_id,
            question=c.question,
            domain=c.domain,
            ask=price,
            net_yield_pct=net_yield_pct,
            size_usd=round(size_usd, 4),
            status="pending",
            capital_before=round(capital_before, 4),
            end_ts=c.end_ts,
        )
        engine = get_nearct_engine(self._settings)
        # Retry on 'database is locked' — losing this INSERT drops the fill.
        import time as _time

        last_exc: Exception | None = None
        for attempt in range(4):
            try:
                with Session(engine) as session:
                    session.add(order)
                    session.commit()
                break
            except Exception as exc:
                last_exc = exc
                log.warning("nearct_place_write_retry %d/4: %s", attempt + 1, exc)
                _time.sleep(2.0 * (attempt + 1))
        else:
            raise last_exc  # type: ignore[misc]

        log.info(
            "nearct_paper: %s price=%.4f size=$%.4f yield=%.2f%% end=%s",
            c.condition_id[:12],
            price,
            size_usd,
            net_yield_pct * 100,
            c.end_ts.strftime("%H:%M"),
        )
        return {
            "id": order_id,
            "condition_id": c.condition_id,
            "token_id": c.token_id,
            "question": c.question,
            "domain": c.domain,
            "ask": price,
            "net_yield_pct": net_yield_pct,
            "size_usd": round(size_usd, 4),
            "end_ts": c.end_ts,
        }

    def _resolve_order(self, order_id: str, result: str, pnl: float, capital_after: float) -> None:
        """Write the order outcome. Retries on 'database is locked': losing this
        write crashes the loop and forces a resume cycle — cheaper to wait out
        the discovery writer than to restart the engine."""
        import time as _time

        from sqlmodel import Session

        from poly_bot.nearct.schema import NearctOrder, get_nearct_engine

        engine = get_nearct_engine(self._settings)
        last_exc: Exception | None = None
        for attempt in range(4):
            try:
                with Session(engine) as session:
                    order = session.get(NearctOrder, order_id)
                    if order is None:
                        return
                    order.status = result
                    order.pnl_usd = pnl
                    order.capital_after = capital_after
                    order.resolved_at = datetime.now(UTC)
                    session.add(order)
                    session.commit()
                return
            except Exception as exc:
                last_exc = exc
                log.warning("nearct_resolve_write_retry %d/4: %s", attempt + 1, exc)
                _time.sleep(2.0 * (attempt + 1))
        raise last_exc  # type: ignore[misc]

    def _load_session_stats(self) -> None:
        """Rebuild status counters from DB after a restart."""
        from sqlalchemy import text

        from poly_bot.nearct.schema import get_nearct_engine

        engine = get_nearct_engine(self._settings)
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                SELECT
                  SUM(CASE WHEN status IN ('won','lost') THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status = 'lost' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status = 'stuck' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status IN ('won','lost')
                       AND date(resolved_at) = date('now') THEN 1 ELSE 0 END),
                  COALESCE(SUM(CASE WHEN status IN ('won','lost')
                                    THEN pnl_usd ELSE 0 END), 0)
                FROM nearct_orders
            """)
            ).fetchone()
        if row is None:
            return
        self._status.total_trades = int(row[0] or 0)
        self._status.wins = int(row[1] or 0)
        self._status.losses = int(row[2] or 0)
        self._status.stuck = int(row[3] or 0)
        self._status.trades_today = int(row[4] or 0)
        self._status.total_pnl = round(float(row[5] or 0.0), 4)

    def _reap_stuck(self) -> None:
        """Re-check timed-out orders onchain; record the late outcome.

        Uses 'won_late'/'lost_late' so _load_capital never confuses a late
        resolution with the live compounding chain. capital_after is left at
        the reset value written when the order went stuck.
        """
        from sqlmodel import Session, select

        from poly_bot.nearct.schema import NearctOrder, get_nearct_engine

        engine = get_nearct_engine(self._settings)
        with Session(engine) as session:
            stuck = session.exec(select(NearctOrder).where(NearctOrder.status == "stuck")).all()
            if not stuck:
                return
            from poly_bot.persistence.db import get_engine as _get_catalog_engine

            catalog_engine = _get_catalog_engine(self._settings)
            for order in stuck:
                idx = _token_outcome_index_sync(catalog_engine, order.token_id)
                if idx is None:
                    continue
                result = self._resolver.check_sync(order.condition_id, idx)
                if result is None:
                    continue
                order.status = f"{result}_late"
                order.pnl_usd = (
                    round(order.size_usd * order.net_yield_pct, 4)
                    if result == "won"
                    else -round(order.size_usd, 4)
                )
                order.resolved_at = datetime.now(UTC)
                session.add(order)
                log.info(
                    "nearct_reaper: stuck order %s resolved late -> %s", order.id[:8], order.status
                )
            session.commit()


# ── module-level helpers ───────────────────────────────────────────────


def _parse_dt(val: object) -> datetime:
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=UTC)
    s = str(val).replace("T", " ").split(".")[0].split("+")[0].strip()
    from datetime import datetime as _dt

    return _dt.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def _token_outcome_index_sync(engine: object, token_id: str) -> int | None:
    """Look up the CTF outcome slot for a token — needed for onchain payout reads."""
    from sqlmodel import Session, select

    from poly_bot.persistence.schema import Token

    try:
        with Session(engine) as session:  # type: ignore[arg-type]
            token = session.exec(select(Token).where(Token.token_id == token_id)).first()
            return token.outcome_index if token else None
    except Exception as exc:
        log.debug("nearct_outcome_index_error: %s", exc)
        return None


def _check_resolution_sync(engine: object, condition_id: str, token_id: str) -> str | None:
    """Return 'won'/'lost' if market closed, else None. Runs in thread pool."""
    import json as _json

    from sqlmodel import Session, select

    from poly_bot.persistence.schema import Market, Token

    with Session(engine) as session:  # type: ignore[arg-type]
        market = session.get(Market, condition_id)
        if market is None or not market.closed:
            return None

        prices = market.raw_payload.get("outcomePrices")
        if isinstance(prices, str):
            try:
                prices = _json.loads(prices)
            except Exception:
                return None
        if not prices or not isinstance(prices, list):
            return None

        winning_index: int | None = None
        for i, p in enumerate(prices):
            try:
                if float(p) >= 0.99:
                    winning_index = i
                    break
            except (TypeError, ValueError):
                continue

        if winning_index is None:
            return None

        tokens = session.exec(select(Token).where(Token.condition_id == condition_id)).all()
        winner = next((t for t in tokens if t.outcome_index == winning_index), None)
        if winner is None:
            return None

        return "won" if winner.token_id == token_id else "lost"
