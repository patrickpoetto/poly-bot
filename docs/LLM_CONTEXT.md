# poly_bot — LLM context (certainty harvest)

Paste this file (or link it) at the start of a session when working on harvest, catalog, or strategy.

You work on **poly_bot**: Python 3.13 Polymarket platform. Phase 1 (infra/catalog) ~done. Trading execution = Phase 2, not built. Current work: **Strategy B — certainty harvest** (scan + paper only, no live orders).

## Objective

Rotate ~$300 by buying near-certain outcomes at **98–99¢** before resolution. Target **~1–2% net/trade** after taker fees.

- **Virtual certainty (approved):** high ask + little time left + real liquidity + fee math.
- **NOT allowed:** external oracles (WU, live scores) to declare certainty.
- **Explicitly rejected:** early "No" on weather/sports bins at 90–96¢ (lost money).

## Architecture

```
discovery/     Gamma crawl → markets, tokens, fees, taxonomy → Postgres
catalog/       Expiring window, REST enrich, WS quotes, dashboard
harvest/       Strategy B: scan, diagnose, pricing, book sanity, paper
exchange/      CLOB + Data public REST (no L2 auth yet)
persistence/   SQLModel, Alembic, sqlite/postgres dialect
core/fees.py   Taker fee math per domain/schedule
weather/       Separate forecast UI/catalog (not harvest core)
ops/           health, reconcile, backup, telegram alerter
cli.py         typer: discovery | catalog | harvest | weather | ops
```

**Pipeline:** `discovery serve` → DB (~120k markets) → `catalog-watch` + `ws-daemon` → `token_quotes` → `harvest scan` (live CLOB `/book`).

**Docker:** `docker/docker-compose.prod.yml`, profiles `discovery`, `heavy`, `ops`, `weather`. Image `poly-bot:0.1.6-dev`.

## Strategy B — parameters

| Param | Default | Env |
|-------|---------|-----|
| Buy band | 98.3–99.5¢ | `HARVEST_MIN_BUY_PRICE=0.983`, `HARVEST_MAX_BUY_PRICE=0.995` |
| Min net yield | 1.2% | `HARVEST_MIN_NET_YIELD_PCT=0.012` |
| Order size | $10 | `HARVEST_ORDER_USD` |
| Min fill depth | $9.5 | `HARVEST_MIN_FILL_USD` |
| Catalog window | 6h | `CATALOG_EXPIRING_HOURS` |
| HOT tier | 2h | `CATALOG_QUOTE_HOT_HOURS` |
| Sports in-play | 4h after kickoff | `CATALOG_SPORTS_IN_PLAY_HOURS` |
| Max book age | 300s | `HARVEST_MAX_BOOK_AGE_SECONDS` |
| Complement | ask + other_bid ≤ 1.02 | `HARVEST_MAX_COMPLEMENT_SUM` |
| Spread (sports) | ask−bid ≤ 5¢ | `HARVEST_SPORTS_MAX_BID_ASK_SPREAD=0.05` |
| Spread (non-sports) | off | `HARVEST_MAX_BID_ASK_SPREAD=0` |

**Price used:** taker **ask** (VWAP ask for fill size). Sort by `window_end_ts` asc.

## Time windows (critical)

**Sports `hours_left` = time until `kickoff + 4h`**, not kickoff.

**HOT rules** (`catalog/time_window.py`):

- Non-sports: `hours_left ≤ CATALOG_QUOTE_HOT_HOURS`
- Sports **in-play**: HOT from kickoff through end of 4h window
- Sports **pre-match**: HOT when `hours_to_kickoff ≤ HOT hours`

**Catalog SQL filter:** `active`, `closed=0`, `accepting_orders=1`, `enable_order_book=1`.

## Harvest live pipeline

```
list_expiring_markets
→ in_hot_window (if hot_only)
→ CLOB GET /book per token
→ reject if !book_fetched | book_age > 300s | no ask
→ price ∈ [0.983, 0.995]
→ net ≥ 1.2% (core/fees.py)
→ book_sanity: complement (+ sports spread)
→ fillable_usd ≥ 9.5 from real ask levels (no invented depth)
→ HarvestOpportunity
```

Live scan does **not** fall back to DB bid/ask when book returned 200.

## Known traps (handled)

| Trap | Mitigation |
|------|------------|
| Losing outcome, ask 98¢, bid 2¢ | complement + sports spread |
| Finished match, zombie CLOB book | `HARVEST_MAX_BOOK_AGE_SECONDS`, `accepting_orders` filter |
| Exact score / resolved @ 99.9–100¢ | outside band; net ~0% |
| DB mid looks good, ask is not | only `harvest scan` live is tradable truth |
| Weather wide spread (~15¢) | no global spread cap; complement only |

## Key files

| Path | Purpose |
|------|---------|
| `harvest/scan.py` | `_evaluate_side`, live + DB scan |
| `harvest/quotes.py` | `SideQuote`, `book_age_s`, `book_fetched` |
| `harvest/book_sanity.py` | complement + per-domain spread |
| `harvest/diagnose.py` | funnel + near-miss reasons |
| `catalog/expiring.py` | window SQL + `hours_left` |
| `catalog/time_window.py` | `in_hot_window()` |
| `catalog/window_stats.py` | funnel stats, `--show-band` |
| `catalog/dashboard.py` | `/harvest` (DB-only, not live) |
| `persistence/dialect.py` | `active_market_filter`, `catalog_window_filter` |
| `settings.py` | env defaults |

## CLI

```bash
poly-bot catalog window-stats [--show-band] [--hot]
poly-bot harvest scan                    # HOT, live (default)
poly-bot harvest scan --all-window
poly-bot harvest diagnose
poly-bot catalog enrich --force -h 6
poly-bot discovery serve                 # refresh closed/accepting_orders
```

Paper log: `data/harvest_paper.jsonl` via `harvest scan --log`.

## State of the art (~2026-06-07)

- DB ~120k total, ~42k active; catalog 6h ~250–340; HOT ~130–290 (mostly live sports)
- Live harvest: usually **0–1** opps in HOT; rest fail band / sanity / liquidity / 99.9¢
- No CLOB L2 / wallet / order placement (`ROADMAP.md` Phase 2)
- Dashboard `/harvest` uses DB mids only
- WS quote horizon ~96h (weather); harvest uses 6h catalog + HOT 2h

## Code rules

- Small focused diffs; match typer / structlog / SQLModel conventions
- No external certainty feeds unless user asks
- Never loosen book sanity for sports without explicit approval
- Don't apply tight spread globally (weather needs wide books)
- Tests: `pytest tests/unit/test_harvest.py tests/unit/test_time_window.py`
- Rebuild Docker after changes: `poly-bot:0.1.6-dev`

## Phase 2 (next, not started)

1. Wallet + CLOB L2 auth
2. Orders from `HarvestOpportunity` (paper first)
3. Risk limits, PnL, reconcile vs CLOB
4. Paper soak → small live capital
