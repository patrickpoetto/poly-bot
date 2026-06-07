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

## Phase status

- **Phase 1 (infra / catalog)** — complete. DB, discovery, weather, harvest scan, book sanity, diagnostics.
- **Phase 2 (execution)** — not started. See `docs/LLM_CONTEXT.md` § "Phase 2 roadmap".
