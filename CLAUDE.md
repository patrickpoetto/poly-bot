# poly_bot — Claude session primer

**Start every session by reading `docs/LLM_CONTEXT.md`** — it covers strategy, architecture, pipeline, parameters, known traps, and Phase 2 roadmap.

## Quick commands

```bash
source .venv/bin/activate

# Tests
python -m pytest -q

# Type check (exclude known-ignored files)
mypy src --exclude "catalog/dashboard|metrics"

# Lint / format
ruff check src tests
ruff format src tests

# CLI entry point
poly-bot --help

# Key harvest commands
poly-bot harvest scan             # live CLOB scan (HOT window markets)
poly-bot harvest scan --db-only   # DB quotes only, no live CLOB calls
poly-bot harvest diagnose         # funnel diagnostics — why 0 opportunities?
poly-bot harvest paper            # dry-run paper log

# Key catalog commands
poly-bot catalog window-stats     # HOT/WARM/ALL market funnel counts
poly-bot catalog expiring         # markets resolving soon
```

## Dashboard

```bash
poly-bot catalog dashboard   # starts on http://localhost:8765
# auto-starts: discovery (delta every ~60s), enrich (every 60s), reconcile (hourly)
# kill: kill -9 $(lsof -ti :8765)
```

**Architecture**: markets sorted by `end_ts ASC` (top 100), live CLOB prices via SSE every 3s for top 20 markets. DB quotes = seed only; live prices override via `applyPrices()`. "Quotes stale" in topbar = DB enrich lag (harmless — live prices bypass DB).

**Known issue**: SQLite `database is locked` when discovery + enrich write concurrently → `busy_timeout=30000` in `db.py` mitigates but doesn't eliminate. CLOB `ConnectError` causes enrich to fail with tenacity RetryError — transient, recovers next cycle.

## Phase status

- **Phase 1 (infra / catalog)** — complete. DB, discovery, weather, harvest scan, book sanity, diagnostics.
- **Phase 1.5 (dashboard)** — complete. Live SSE dashboard, auto-background tasks, sticky header, price flash.
- **Phase 2 (execution)** — **next**. See `docs/LLM_CONTEXT.md` § "Phase 2 roadmap".
