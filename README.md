# poly_bot

Production-grade Polymarket platform: **living market catalog** first, then strategies and execution.

## Stack

- Python 3.13, `pyproject.toml` (PEP 621)
- **SQLite or PostgreSQL** + SQLModel + Alembic migrations
- **httpx** async — Gamma / CLOB / Data API
- **structlog**, **pydantic-settings**, **typer**, **rich**, optional **FastAPI** dashboard

## Quick start (local / SQLite)

```bash
cd poly_bot
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,dashboard]"

cp .env.example .env
poly-bot discovery init-db
poly-bot discovery scan
poly-bot discovery scan-taxonomy
poly-bot catalog link
poly-bot discovery backfill-fees

# Live platform
poly-bot run                       # discovery scheduler + catalog watch
poly-bot catalog dashboard         # http://127.0.0.1:8765

# Ops
poly-bot ops reconcile             # data-quality report
poly-bot ops backup                # SQLite copy → data/backups/
poly-bot catalog reclassify        # re-run classifier on unknown domains
```

## Production (Docker + Postgres)

```bash
cp .env.example .env
# set POSTGRES_PASSWORD

docker compose -f docker/docker-compose.prod.yml up -d
docker compose -f docker/docker-compose.prod.yml ps   # healthchecks
# dashboard → http://localhost:8765
# metrics   → http://localhost:8765/metrics (METRICS_ENABLED=true)
```

Stack:

| Service         | Command                       | Restart      | Healthcheck                        |
|-----------------|-------------------------------|--------------|------------------------------------|
| `postgres`      | (image)                       | unless-stopped | `pg_isready`                     |
| `migrate`       | `poly-bot ops migrate`        | one-shot     | run-to-completion                  |
| `discovery`     | `poly-bot discovery serve`    | on-failure:5 | `poly-bot ops health --service discovery` |
| `catalog-watch` | `poly-bot catalog watch`      | on-failure:5 | `poly-bot ops health --service watch`     |
| `ws-daemon`     | `poly-bot catalog ws-daemon`  | on-failure:5 | `poly-bot ops health --service ws`        |
| `dashboard`     | `poly-bot catalog dashboard`  | on-failure:5 | HTTP `/health`                     |
| `backup`        | `poly-bot ops backup-loop`    | on-failure:5 | (writes to `pg_backups` volume)    |

All non-DB services run as **uid 1000**, **read-only FS** (except backup), `mem_limit`, JSON log rotation. `postgres` and `backup` only reachable on the **backend** network; only the **dashboard** is exposed to the host.

Pin a published image with `POLY_BOT_IMAGE=ghcr.io/<user>/poly-bot:0.1.0` in `.env`.

Local dev compose (SQLite volume): `docker compose -f docker/docker-compose.yml up`

## Discovery pipeline

| Stage | Status | Description |
|-------|--------|-------------|
| Gamma markets (full) | ✅ | keyset pagination |
| Gamma markets (delta) | ✅ | `updatedAt` watermark |
| Events + series | ✅ | keyset + offset; series refresh on delta |
| Fees (market + event) | ✅ | Gamma `feeSchedule`, event rollup |
| Taxonomy link | ✅ | tags, domain, series slug |
| Re-classify | ✅ | `catalog reclassify` |
| Scheduler | ✅ | `discovery serve` / `poly-bot run` |

## Live catalog

| Feature | Status |
|---------|--------|
| Expiring window (6h default) | ✅ |
| CLOB quotes + WS burst | ✅ |
| Open interest (Data API) | ✅ |
| Stale-only enrich | ✅ |
| Dashboard (filters, events, fees) | ✅ |
| SSE stream `/api/stream` | ✅ |
| Prometheus `/metrics` | ✅ (optional) |

## Ops

| Command | Purpose |
|---------|---------|
| `poly-bot ops migrate` | Alembic (Postgres) or SQLite schema |
| `poly-bot ops health [--service ...]` | DB ping + activity freshness (exit 0/1) |
| `poly-bot ops reconcile` | NULL fees, missing tokens, unknown domains |
| `poly-bot ops backup` | One-shot SQLite copy or `pg_dump` |
| `poly-bot ops backup-loop --interval 3600 --retention-days 7` | Sidecar daemon |
| `poly-bot catalog reclassify` | Batch classifier refresh |
| `poly-bot catalog taxonomy-add KIND KEY --domain ...` | Manual override |
| `poly-bot catalog ws-daemon` | Persistent CLOB WebSocket |

## Fees

Formula (taker): `fee_per_share = rate × p × (1-p)` — see `src/poly_bot/core/fees.py`.

## Development

```bash
pip install -e ".[dev,dashboard,postgres]"
ruff check src tests && ruff format src tests
mypy src
pytest
```

Postgres locally:

```bash
export DATABASE_URL=postgresql+psycopg://poly_bot:secret@localhost:5432/poly_bot
poly-bot ops migrate
poly-bot discovery scan
```
