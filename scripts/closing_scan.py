"""Standalone helper — scans Polymarket for near-close yield opportunities.

Isolated from the rest of poly_bot on purpose: no DB, no dashboard, no
imports from `poly_bot.*`. Talks straight to the public Gamma + CLOB REST
APIs so it keeps working even if the main pipeline is mid-refactor.

Finds every market closing in the next N minutes, drops the noisy crypto
5m/15m/1h "Up or Down" series (informed flow near close — see NearCT
postmortems), and flags any outcome token whose net yield (ask price,
fee-adjusted) lands inside [--yield-min, --yield-max].

Every newly-seen opportunity (by token_id) is appended once to a local JSONL
log (default data/closing_scan_log.jsonl) — Telegram alerts and the log only
fire on first sighting, never on repeat cycles for the same still-open token.
Use --report to summarize that log: density and absorbable liquidity over a
given window, the two numbers that actually matter for sizing this strategy.

Usage:
    python scripts/closing_scan.py                       # one shot
    python scripts/closing_scan.py --watch 20             # poll every 20s
    python scripts/closing_scan.py --watch 20 --telegram  # + alert on NEW hits
    python scripts/closing_scan.py --yield-min 0.006 --yield-max 0.03
    python scripts/closing_scan.py --json                 # machine-readable
    python scripts/closing_scan.py --report --report-hours 24

Needs TELEGRAM_TOKEN / TELEGRAM_CHAT_ID in .env for --telegram.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import itertools
import json as jsonlib
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

GAMMA_HOST = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"
TELEGRAM_API = "https://api.telegram.org"

# Noisy crypto series to drop — recur every 5m/15m/1h, dominated by informed
# flow right at close. Longer-horizon crypto (4h+) and everything else stays.
# Two slug families exist for the same thing: the rolling-window one
# (btc-updown-1h-<epoch>) and a clock-aligned one for the hourly cadence
# (bitcoin-up-or-down-july-26-2026-4pm-et) — both must be excluded.
_NOISY_CRYPTO_RE = re.compile(r"^[a-z]+-updown-(5m|15m|1h)-\d+$", re.I)
_NOISY_CRYPTO_HOURLY_ET_RE = re.compile(
    r"^[a-z]+-up-or-down-[a-z]+-\d{1,2}-\d{4}-\d{1,2}(am|pm)-et$", re.I
)

_DEFAULT_FEE_RATE = 0.07
_MAX_PAGES = 40
_PAGE_LIMIT = 100
_BOOK_CHUNK = 80


@dataclass
class Hit:
    question: str
    outcome: str
    slug: str
    token_id: str
    condition_id: str
    price_lo: float  # cheapest price in the qualifying yield band (best entry)
    price_hi: float  # richest price in the qualifying yield band
    yield_pct: float  # net yield at price_lo
    depth_usd: float
    end_ts: datetime
    minutes_left: float
    complement_token_id: str | None  # cheapest other outcome in the same market
    complement_ask: float | None
    complement_bid: float | None


def _parse_iso(value: object) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _parse_json_list(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = jsonlib.loads(value)
            return parsed if isinstance(parsed, list) else []
        except jsonlib.JSONDecodeError:
            return []
    return []


def _fee_rate_exponent(raw: dict[str, Any]) -> tuple[float, float]:
    if not raw.get("feesEnabled", raw.get("fees_enabled", True)):
        return 0.0, 1.0
    schedule = raw.get("feeSchedule") or raw.get("fee_schedule")
    if isinstance(schedule, dict) and schedule.get("rate") is not None:
        try:
            return float(schedule["rate"]), float(schedule.get("exponent") or 1.0)
        except (TypeError, ValueError):
            pass
    return _DEFAULT_FEE_RATE, 1.0


def _net_yield(price: float, rate: float, exponent: float) -> float:
    """Net return per $1 of cost at a given ask price (Polymarket fee curve)."""
    if price <= 0.0 or price >= 1.0:
        return 0.0
    fee = rate * min(price, 1.0 - price) ** exponent
    cost = price + fee
    return (1.0 - cost) / cost


async def fetch_closing_markets(
    client: httpx.AsyncClient, horizon_min: float
) -> list[dict[str, Any]]:
    """All active markets whose endDate falls within the next horizon_min."""
    now = datetime.now(UTC)
    horizon = now + timedelta(minutes=horizon_min)
    out: list[dict[str, Any]] = []
    offset = 0

    for _ in range(_MAX_PAGES):
        resp = await client.get(
            "/markets",
            params={
                "active": "true",
                "closed": "false",
                "order": "endDate",
                "ascending": "true",
                "limit": _PAGE_LIMIT,
                "offset": offset,
                "end_date_min": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_date_max": horizon.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
        resp.raise_for_status()
        payload = resp.json()
        items = payload if isinstance(payload, list) else payload.get("data") or []
        if not items:
            break

        reached_horizon = False
        for m in items:
            end_ts = _parse_iso(m.get("endDate") or m.get("end_date"))
            if end_ts is None:
                continue
            if end_ts > horizon:
                reached_horizon = True
                break
            if end_ts < now:
                continue
            out.append(m)

        if reached_horizon or len(items) < _PAGE_LIMIT:
            break
        offset += _PAGE_LIMIT

    return out


def _is_noisy_crypto(slug: str) -> bool:
    slug = slug or ""
    return bool(_NOISY_CRYPTO_RE.match(slug) or _NOISY_CRYPTO_HOURLY_ET_RE.match(slug))


# Observation-window markets (temperature, air quality, etc.) close betting at
# end_ts but the real UMA resolution lands 8-20h later — "closing soon" here
# does NOT mean "resolving soon". Off by default (user asked only to drop
# noisy crypto); pass --exclude-slow-resolve to also drop these.
_SLOW_RESOLVE_KEYWORDS = (
    "temperature",
    "°f",
    "°c",
    "degrees",
    "rainfall",
    "precipitation",
    "humidity",
    "wind speed",
    "wind gust",
    "uv index",
    "air quality",
    "aqi",
    "snowfall",
    "highest temp",
    "lowest temp",
    "high temp",
    "low temp",
)


def _is_slow_resolve(question: str) -> bool:
    q = question.lower()
    return any(kw in q for kw in _SLOW_RESOLVE_KEYWORDS)


@dataclass
class _TokenCtx:
    token_id: str
    condition_id: str
    question: str
    slug: str
    outcome: str
    end_ts: datetime
    fee_rate: float
    fee_exponent: float


def build_token_contexts(
    markets: list[dict[str, Any]], *, exclude_slow_resolve: bool = False
) -> list[_TokenCtx]:
    out: list[_TokenCtx] = []
    for m in markets:
        slug = str(m.get("slug") or "")
        if _is_noisy_crypto(slug):
            continue
        if not m.get("acceptingOrders", m.get("accepting_orders", True)):
            continue
        if not m.get("enableOrderBook", m.get("enable_order_book", True)):
            continue

        question = str(m.get("question") or m.get("title") or "")
        if exclude_slow_resolve and _is_slow_resolve(question):
            continue
        end_ts = _parse_iso(m.get("endDate") or m.get("end_date"))
        if end_ts is None:
            continue

        condition_id = str(m.get("conditionId") or m.get("condition_id") or "")
        if not condition_id:
            continue
        outcomes = _parse_json_list(m.get("outcomes"))
        token_ids = _parse_json_list(m.get("clobTokenIds") or m.get("clob_token_ids"))
        if not token_ids:
            continue
        rate, exponent = _fee_rate_exponent(m)

        for i, tid in enumerate(token_ids):
            label = str(outcomes[i]) if i < len(outcomes) else f"outcome_{i}"
            out.append(
                _TokenCtx(
                    token_id=str(tid),
                    condition_id=condition_id,
                    question=question,
                    slug=slug,
                    outcome=label,
                    end_ts=end_ts,
                    fee_rate=rate,
                    fee_exponent=exponent,
                )
            )
    return out


def _book_levels(raw: object) -> list[tuple[float, float]]:
    """Parse book side entries into (price, size), sorted ascending by price."""
    if not isinstance(raw, list):
        return []
    levels: list[tuple[float, float]] = []
    for entry in raw:
        try:
            levels.append((float(entry["price"]), float(entry.get("size", 0))))
        except (KeyError, TypeError, ValueError):
            continue
    levels.sort(key=lambda x: x[0])
    return levels


async def fetch_books(client: httpx.AsyncClient, token_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Raw book dict (bids/asks) per token_id, batched with a fallback."""
    if not token_ids:
        return {}

    result: dict[str, dict[str, Any]] = {}
    unique = list(dict.fromkeys(token_ids))

    for i in range(0, len(unique), _BOOK_CHUNK):
        chunk = unique[i : i + _BOOK_CHUNK]
        try:
            resp = await client.post("/books", json=[{"token_id": t} for t in chunk])
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    tid = item.get("asset_id") or item.get("token_id")
                    if tid:
                        result[str(tid)] = item
        except httpx.HTTPError:
            pass

    missing = [t for t in unique if t not in result]
    if missing:
        sem = asyncio.Semaphore(20)

        async def _one(tid: str) -> None:
            async with sem, contextlib.suppress(httpx.HTTPError):
                r = await client.get("/book", params={"token_id": tid})
                r.raise_for_status()
                result[tid] = r.json()

        await asyncio.gather(*[_one(t) for t in missing])

    return result


def find_hits(
    contexts: list[_TokenCtx],
    books: dict[str, dict[str, Any]],
    *,
    yield_min: float,
    yield_max: float,
) -> list[Hit]:
    now = datetime.now(UTC)
    hits: list[Hit] = []

    by_condition: dict[str, list[_TokenCtx]] = {}
    for c in contexts:
        by_condition.setdefault(c.condition_id, []).append(c)

    for ctx in contexts:
        book = books.get(ctx.token_id)
        if not book:
            continue
        asks = _book_levels(book.get("asks"))
        if not asks:
            continue

        # Ask levels sorted ascending by price == descending by yield.
        # Sum $ depth across the contiguous band whose yield sits in range,
        # and track the price extremes of that band (not top-of-book —
        # the best ask is often a near-worthless dust order on thin markets).
        depth_usd = 0.0
        price_lo: float | None = None
        price_hi: float | None = None
        for price, size in asks:
            y = _net_yield(price, ctx.fee_rate, ctx.fee_exponent)
            if y > yield_max:
                continue
            if y < yield_min:
                break
            depth_usd += price * size
            if price_lo is None:
                price_lo = price
            price_hi = price

        if depth_usd <= 0 or price_lo is None or price_hi is None:
            continue

        # Cheapest other outcome in the same market — the "long shot" side.
        # Its book was already fetched (we pull books for every outcome of
        # every candidate market), so this costs no extra requests.
        comp_token_id: str | None = None
        comp_ask: float | None = None
        comp_bid: float | None = None
        for sibling in by_condition.get(ctx.condition_id, []):
            if sibling.token_id == ctx.token_id:
                continue
            sib_book = books.get(sibling.token_id)
            if not sib_book:
                continue
            sib_asks = _book_levels(sib_book.get("asks"))
            sib_bids = _book_levels(sib_book.get("bids"))
            sib_ask = sib_asks[0][0] if sib_asks else None
            sib_bid = sib_bids[-1][0] if sib_bids else None
            if sib_ask is not None and (comp_ask is None or sib_ask < comp_ask):
                comp_token_id, comp_ask, comp_bid = sibling.token_id, sib_ask, sib_bid

        minutes_left = max(0.0, (ctx.end_ts - now).total_seconds() / 60.0)
        hits.append(
            Hit(
                question=ctx.question,
                outcome=ctx.outcome,
                slug=ctx.slug,
                token_id=ctx.token_id,
                condition_id=ctx.condition_id,
                price_lo=price_lo,
                price_hi=price_hi,
                yield_pct=_net_yield(price_lo, ctx.fee_rate, ctx.fee_exponent),
                depth_usd=depth_usd,
                end_ts=ctx.end_ts,
                minutes_left=minutes_left,
                complement_token_id=comp_token_id,
                complement_ask=comp_ask,
                complement_bid=comp_bid,
            )
        )

    hits.sort(key=lambda h: -h.yield_pct)
    return hits


def _fmt_row(h: Hit) -> str:
    q = (h.question[:44] + "…") if len(h.question) > 45 else h.question
    outcome = h.outcome[:12]
    band = f"{h.price_lo:.4f}-{h.price_hi:.4f}"
    return (
        f"{q:<45} {outcome:<12} {band:>15} "
        f"{h.yield_pct * 100:>6.2f}% ${h.depth_usd:>9,.0f} {h.minutes_left:>5.1f}m"
    )


def print_report(
    hits: list[Hit],
    *,
    scanned: int,
    kept: int,
    yield_min: float,
    yield_max: float,
    limit: int,
    new_count: int | None = None,
) -> None:
    now = datetime.now(UTC).strftime("%H:%M:%S")
    new_suffix = f" ({new_count} new)" if new_count is not None else ""
    print(
        f"[{now}] scanned {scanned} markets closing soon, {kept} after dropping "
        f"noisy crypto 5m/15m/1h -> {len(hits)} hit(s) in "
        f"[{yield_min * 100:.2f}%, {yield_max * 100:.2f}%]{new_suffix}"
    )
    if not hits:
        return
    header = (
        f"{'MARKET':<45} {'OUTCOME':<12} {'PRICE BAND':>15} "
        f"{'YIELD':>7} {'DEPTH$ IN RANGE':>10} {'CLOSES':>6}"
    )
    print(header)
    print("-" * len(header))
    for h in hits[:limit]:
        print(_fmt_row(h))
    if len(hits) > limit:
        print(f"... and {len(hits) - limit} more (--json for the full list, --limit to raise this)")


def hits_to_json(hits: list[Hit]) -> str:
    return jsonlib.dumps(
        [
            {
                "question": h.question,
                "outcome": h.outcome,
                "slug": h.slug,
                "token_id": h.token_id,
                "condition_id": h.condition_id,
                "price_lo": h.price_lo,
                "price_hi": h.price_hi,
                "yield_pct": h.yield_pct,
                "depth_usd": h.depth_usd,
                "end_ts": h.end_ts.isoformat(),
                "minutes_left": h.minutes_left,
                "complement_token_id": h.complement_token_id,
                "complement_ask": h.complement_ask,
                "complement_bid": h.complement_bid,
            }
            for h in hits
        ],
        indent=2,
    )


def default_log_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "closing_scan_log.jsonl"


def load_seen_tokens(path: Path) -> set[str]:
    if not path.exists():
        return set()
    seen: set[str] = set()
    with path.open() as f:
        for line in f:
            with contextlib.suppress(jsonlib.JSONDecodeError):
                row = jsonlib.loads(line)
                tid = row.get("token_id")
                if tid:
                    seen.add(tid)
    return seen


def append_log(path: Path, hits: list[Hit]) -> None:
    if not hits:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    now_iso = datetime.now(UTC).isoformat()
    with path.open("a") as f:
        for h in hits:
            row = {
                "logged_at": now_iso,
                "question": h.question,
                "outcome": h.outcome,
                "slug": h.slug,
                "token_id": h.token_id,
                "condition_id": h.condition_id,
                "price_lo": h.price_lo,
                "price_hi": h.price_hi,
                "yield_pct": h.yield_pct,
                "depth_usd": h.depth_usd,
                "end_ts": h.end_ts.isoformat(),
                "complement_token_id": h.complement_token_id,
                "complement_ask": h.complement_ask,
                "complement_bid": h.complement_bid,
            }
            f.write(jsonlib.dumps(row) + "\n")


def default_resolutions_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "closing_scan_resolutions.jsonl"


async def _fetch_market_winners(
    client: httpx.AsyncClient, condition_id: str
) -> dict[str, bool] | None:
    """token_id -> winner, or None if the market isn't resolved (closed) yet."""
    try:
        r = await client.get(f"/markets/{condition_id}")
        r.raise_for_status()
        data = r.json()
    except httpx.HTTPError:
        return None
    if not isinstance(data, dict) or not data.get("closed"):
        return None
    tokens = data.get("tokens")
    if not isinstance(tokens, list):
        return None
    out: dict[str, bool] = {}
    for t in tokens:
        if isinstance(t, dict) and t.get("token_id") is not None:
            out[str(t["token_id"])] = bool(t.get("winner"))
    return out or None


async def _fetch_all_winners(condition_ids: set[str]) -> dict[str, dict[str, bool] | None]:
    winners_by_condition: dict[str, dict[str, bool] | None] = {}
    sem = asyncio.Semaphore(20)

    async with httpx.AsyncClient(base_url=CLOB_HOST, timeout=15.0) as client:

        async def _one(cid: str) -> None:
            async with sem:
                winners_by_condition[cid] = await _fetch_market_winners(client, cid)

        await asyncio.gather(*[_one(cid) for cid in condition_ids])

    return winners_by_condition


def resolve_pending(log_path: Path, resolutions_path: Path) -> None:
    """Check CLOB for resolution status of every logged opportunity not yet resolved."""
    if not log_path.exists():
        print(f"No log at {log_path} yet.")
        return

    opportunities: dict[str, dict[str, Any]] = {}
    with log_path.open() as f:
        for line in f:
            with contextlib.suppress(jsonlib.JSONDecodeError):
                row = jsonlib.loads(line)
                tid = row.get("token_id")
                if tid:
                    opportunities[tid] = row

    already: set[str] = set()
    if resolutions_path.exists():
        with resolutions_path.open() as f:
            for line in f:
                with contextlib.suppress(jsonlib.JSONDecodeError):
                    row = jsonlib.loads(line)
                    tid = row.get("token_id")
                    if tid:
                        already.add(tid)

    pending = {
        tid: row
        for tid, row in opportunities.items()
        if tid not in already and row.get("condition_id")
    }
    if not pending:
        print("Nothing new to resolve.")
        return

    condition_ids = {row["condition_id"] for row in pending.values()}
    print(
        f"Checking {len(condition_ids)} market(s) for {len(pending)} pending opportunit(y/ies)..."
    )

    winners_by_condition = asyncio.run(_fetch_all_winners(condition_ids))

    resolutions_path.parent.mkdir(parents=True, exist_ok=True)
    now_iso = datetime.now(UTC).isoformat()
    resolved_count = 0
    with resolutions_path.open("a") as f:
        for tid, row in pending.items():
            winners = winners_by_condition.get(row["condition_id"])
            if winners is None or tid not in winners:
                continue  # not resolved yet — retry on next --resolve run
            comp_tid = row.get("complement_token_id")
            comp_won = winners.get(comp_tid) if comp_tid else None
            f.write(
                jsonlib.dumps(
                    {
                        "token_id": tid,
                        "condition_id": row["condition_id"],
                        "checked_at": now_iso,
                        "won": winners[tid],
                        "complement_token_id": comp_tid,
                        "complement_won": comp_won,
                    }
                )
                + "\n"
            )
            resolved_count += 1

    print(
        f"Resolved {resolved_count}/{len(pending)} (rest not settled yet — rerun --resolve later)."
    )


def print_daily_report(path: Path, hours: float, resolutions_path: Path | None = None) -> None:
    if not path.exists():
        print(f"No log at {path} yet — run the scanner (with logging on) first.")
        return

    cutoff = datetime.now(UTC) - timedelta(hours=hours)
    rows: list[dict[str, Any]] = []
    skipped_noisy = 0
    with path.open() as f:
        for line in f:
            with contextlib.suppress(jsonlib.JSONDecodeError):
                row = jsonlib.loads(line)
                if _is_noisy_crypto(row.get("slug") or ""):
                    skipped_noisy += 1
                    continue
                logged_at = _parse_iso(row.get("logged_at"))
                if logged_at is not None and logged_at >= cutoff:
                    rows.append(row)
    if skipped_noisy:
        print(f"(filtered out {skipped_noisy} noisy-crypto rows logged before the filter fix)")

    print(f"=== density report — last {hours:.1f}h ({len(rows)} distinct opportunities) ===")
    if not rows:
        return

    depths = [float(r["depth_usd"]) for r in rows]
    yields_pct = sorted(float(r["yield_pct"]) * 100 for r in rows)
    total_depth = sum(depths)
    median_yield = yields_pct[len(yields_pct) // 2]

    # Rate uses the actual span of collected data, not the requested window —
    # if the scanner only ran for 1h out of a 24h window, dividing by 24 would
    # understate the true hourly rate.
    logged_ats = [t for r in rows if (t := _parse_iso(r.get("logged_at"))) is not None]
    span_hours = (max(logged_ats) - min(logged_ats)).total_seconds() / 3600 if logged_ats else 0.0
    rate_basis = span_hours if span_hours > 0.05 else hours

    print(f"Total absorbable liquidity: ${total_depth:,.0f}")
    print(f"Avg depth/opportunity:      ${total_depth / len(rows):,.0f}")
    print(f"Median yield:               {median_yield:.2f}%")
    print(
        f"Rate:                       {len(rows) / rate_basis:.1f} opportunities/hour "
        f"(collected over {span_hours:.2f}h of actual runtime)"
    )

    # Gaps between distinct scan cycles that found something — not between
    # individual rows, since one live match can log dozens of correlated
    # rows in the same instant. This is the real answer to "how long can
    # it go quiet": average/rate hides dead zones that a max-gap won't.
    unique_ts = sorted(set(logged_ats))
    if len(unique_ts) > 1:
        gaps_min = [(b - a).total_seconds() / 60.0 for a, b in itertools.pairwise(unique_ts)]
        print(
            f"Longest quiet gap:          {max(gaps_min):.1f} min "
            f"(between distinct hit-clusters, {len(unique_ts)} clusters total)"
        )

    by_hour: dict[str, int] = {}
    for r in rows:
        logged_at = _parse_iso(r.get("logged_at"))
        if logged_at is not None:
            key = logged_at.strftime("%Y-%m-%d %H:00")
            by_hour[key] = by_hour.get(key, 0) + 1
    print("\nPer hour:")
    max_count = max(by_hour.values())
    bar_scale = max(1, max_count / 50)
    for key in sorted(by_hour):
        bar = "#" * max(1, round(by_hour[key] / bar_scale))
        print(f"  {key}  {bar}  {by_hour[key]}")

    print("\nTop 10 by depth:")
    for r in sorted(rows, key=lambda r: -float(r["depth_usd"]))[:10]:
        q = str(r["question"])[:50]
        print(
            f"  ${float(r['depth_usd']):>9,.0f}  {float(r['yield_pct']) * 100:>5.2f}%  "
            f"{q} [{r['outcome']}]"
        )

    if resolutions_path is None or not resolutions_path.exists():
        print("\n(no resolutions yet — run --resolve after markets settle to add win/loss stats)")
        return

    resolutions: dict[str, dict[str, Any]] = {}
    with resolutions_path.open() as f:
        for line in f:
            with contextlib.suppress(jsonlib.JSONDecodeError):
                res_row = jsonlib.loads(line)
                tid = res_row.get("token_id")
                if tid is not None and "won" in res_row:
                    resolutions[tid] = res_row

    resolved_rows = [r for r in rows if r.get("token_id") in resolutions]
    print(f"\n=== outcomes ({len(resolved_rows)}/{len(rows)} resolved) ===")
    if not resolved_rows:
        print("(none of these opportunities have settled yet)")
        return

    wins = sum(1 for r in resolved_rows if resolutions[r["token_id"]]["won"])
    losses = len(resolved_rows) - wins
    win_rate = wins / len(resolved_rows)
    breakevens = [1.0 / (1.0 + float(r["yield_pct"])) for r in resolved_rows]
    avg_breakeven = sum(breakevens) / len(breakevens)

    print(f"Won:                        {wins}")
    print(f"Lost (flipped):             {losses}")
    print(f"Actual win rate:            {win_rate * 100:.2f}%")
    print(f"Breakeven win rate needed:  {avg_breakeven * 100:.2f}% (implied by avg entry yield)")
    print(f"Edge:                       {(win_rate - avg_breakeven) * 100:+.2f}pp")

    if losses:
        print("\nFlipped (lost) outcomes:")
        for r in resolved_rows:
            if not resolutions[r["token_id"]]["won"]:
                q = str(r["question"])[:55]
                print(f"  {q} [{r['outcome']}] — entry yield {float(r['yield_pct']) * 100:.2f}%")

    # Would buying the cheap complement (the "long shot" side) instead have
    # been +EV? We logged its ask at capture time and its own win/loss —
    # this is real, not the inverse of the favorite's result, since
    # multi-outcome markets can have a third side win instead.
    comp_rows = [
        r
        for r in resolved_rows
        if r.get("complement_ask") is not None
        and resolutions[r["token_id"]].get("complement_won") is not None
    ]
    if comp_rows:
        comp_wins = sum(1 for r in comp_rows if resolutions[r["token_id"]]["complement_won"])
        comp_win_rate = comp_wins / len(comp_rows)
        comp_asks = [float(r["complement_ask"]) for r in comp_rows]
        avg_comp_ask = sum(comp_asks) / len(comp_asks)
        # $1 staked on the complement each time: profit = 1/ask - 1 if won, else -1
        rois = [
            (1.0 / float(r["complement_ask"]) - 1.0)
            if resolutions[r["token_id"]]["complement_won"]
            else -1.0
            for r in comp_rows
        ]
        avg_roi = sum(rois) / len(rois)
        print(f'\n=== complement ("long shot" side) check — {len(comp_rows)} pairs ===')
        print(f"Complement win rate:        {comp_win_rate * 100:.2f}%")
        print(f"Avg complement ask price:   {avg_comp_ask:.4f}")
        print(f"Avg ROI per $1 staked:      {avg_roi * 100:+.2f}%")
        if avg_roi > 0:
            print("(positive — but check the sample size before trusting this)")
    else:
        print("\n(no complement data yet — needs opportunities logged after this feature shipped)")


async def send_telegram(hits: list[Hit]) -> None:
    import os

    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id or not hits:
        return

    lines = [f"*{len(hits)} new near-close opportunit{'y' if len(hits) == 1 else 'ies'}*"]
    for h in hits[:15]:
        lines.append(
            f"`{h.yield_pct * 100:.2f}%` {h.question[:60]} [{h.outcome}]"
            f" — {h.price_lo:.4f}-{h.price_hi:.4f}, ${h.depth_usd:,.0f} depth,"
            f" closes {h.minutes_left:.1f}m"
        )
    text = "\n".join(lines)

    async with httpx.AsyncClient(timeout=10.0) as client:
        with contextlib.suppress(httpx.HTTPError):
            await client.post(
                f"{TELEGRAM_API}/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            )


async def run_once(
    *,
    horizon_min: float,
    yield_min: float,
    yield_max: float,
    telegram: bool,
    as_json: bool,
    exclude_slow_resolve: bool,
    limit: int,
    seen: set[str],
    log_path: Path | None,
) -> list[Hit]:
    async with httpx.AsyncClient(base_url=GAMMA_HOST, timeout=15.0) as gamma_client:
        markets = await fetch_closing_markets(gamma_client, horizon_min)

    contexts = build_token_contexts(markets, exclude_slow_resolve=exclude_slow_resolve)

    async with httpx.AsyncClient(base_url=CLOB_HOST, timeout=15.0) as clob_client:
        books = await fetch_books(clob_client, [c.token_id for c in contexts])

    hits = find_hits(contexts, books, yield_min=yield_min, yield_max=yield_max)
    new_hits = [h for h in hits if h.token_id not in seen]
    seen.update(h.token_id for h in new_hits)

    if as_json:
        print(hits_to_json(hits))
    else:
        kept_markets = len({c.slug for c in contexts})
        print_report(
            hits,
            scanned=len(markets),
            kept=kept_markets,
            yield_min=yield_min,
            yield_max=yield_max,
            limit=limit,
            new_count=len(new_hits),
        )

    if log_path is not None:
        append_log(log_path, new_hits)

    if telegram:
        await send_telegram(new_hits)

    return hits


async def main_async(args: argparse.Namespace) -> None:
    log_path = None if args.no_log else Path(args.log)
    seen = load_seen_tokens(log_path) if log_path is not None else set()

    kwargs = {
        "horizon_min": args.horizon_min,
        "yield_min": args.yield_min,
        "yield_max": args.yield_max,
        "telegram": args.telegram,
        "as_json": args.json,
        "exclude_slow_resolve": args.exclude_slow_resolve,
        "limit": args.limit,
        "seen": seen,
        "log_path": log_path,
    }
    if args.watch:
        while True:
            try:
                await run_once(**kwargs)
            except httpx.HTTPError as exc:
                print(f"scan error (will retry): {exc}", file=sys.stderr)
            await asyncio.sleep(args.watch)
    else:
        await run_once(**kwargs)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan Polymarket for near-close yield opportunities."
    )
    parser.add_argument(
        "--horizon-min", type=float, default=5.0, help="Markets closing within N minutes."
    )
    parser.add_argument(
        "--yield-min", type=float, default=0.009, help="Min net yield (0.009 = 0.9%%)."
    )
    parser.add_argument(
        "--yield-max", type=float, default=0.021, help="Max net yield (0.021 = 2.1%%)."
    )
    parser.add_argument(
        "--watch", type=float, default=0.0, help="Poll every N seconds instead of running once."
    )
    parser.add_argument(
        "--telegram",
        action="store_true",
        help="Send NEW hits to TELEGRAM_TOKEN/TELEGRAM_CHAT_ID (deduped, once per token).",
    )
    parser.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON instead of a table."
    )
    parser.add_argument(
        "--exclude-slow-resolve",
        action="store_true",
        help="Also drop temperature/weather markets — end_ts is the betting cutoff, "
        "not resolution (UMA lands 8-20h later).",
    )
    parser.add_argument(
        "--limit", type=int, default=40, help="Max rows to print (table mode only)."
    )
    parser.add_argument(
        "--log",
        default=str(default_log_path()),
        help="JSONL file recording every newly-seen opportunity (for --report).",
    )
    parser.add_argument("--no-log", action="store_true", help="Disable logging entirely.")
    parser.add_argument(
        "--report",
        action="store_true",
        help="Print a density/liquidity/outcome summary from --log and exit (no scanning).",
    )
    parser.add_argument(
        "--report-hours", type=float, default=24.0, help="Window for --report (hours)."
    )
    parser.add_argument(
        "--resolutions",
        default=str(default_resolutions_path()),
        help="JSONL file recording win/loss per token_id (for --resolve and --report).",
    )
    parser.add_argument(
        "--resolve",
        action="store_true",
        help="Check CLOB for resolution status of logged opportunities and exit (no scanning).",
    )
    args = parser.parse_args()

    if args.resolve:
        resolve_pending(Path(args.log), Path(args.resolutions))
        return

    if args.report:
        print_daily_report(Path(args.log), args.report_hours, Path(args.resolutions))
        return

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
