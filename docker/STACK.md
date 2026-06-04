# Docker stack — bandwidth & one service at a time

On a home Mac, **ws-daemon** and **catalog-watch** can saturate Wi‑Fi. Use profiles or the weather-only compose file.

## Weather only (recommended at home)

```bash
# Stop everything heavy (from project root)
docker compose --env-file .env -f docker/docker-compose.prod.yml stop \
  ws-daemon catalog-watch discovery alerter reconcile backup 2>/dev/null || true

# Minimal: DB + dashboard (Weather tab; prices on city page via REST)
docker compose --env-file .env -f docker/docker-compose.prod.yml up -d
```

Or dedicated file (same idea):

```bash
docker compose --env-file .env -f docker/docker-compose.weather.yml up -d
```

Open: http://127.0.0.1:8765/weather

**Outbound traffic:** only when you open a **city** page (~20 CLOB requests every 5s). List page = no Polymarket calls.

### TAF/METAR ingest (all ICAO stations)

```bash
# One-shot after migrate + weather sync
docker compose --env-file .env -f docker/docker-compose.prod.yml run --rm dashboard \
  poly-bot weather aviation-sync

# Or loop every 15m (profile weather)
docker compose --env-file .env -f docker/docker-compose.prod.yml --profile weather up -d weather-aviation
```

Dashboard: `/weather` (card grid + WU station temps), `/weather/aviation` (sources + raw TAF).

### WU resolution station (max giorno mercato)

Requires **image rebuilt** after code changes (`docker build -f docker/Dockerfile -t poly-bot:latest .`).

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml run --rm dashboard \
  poly-bot weather station-sync

# One forecast day only:
docker compose --env-file .env -f docker/docker-compose.prod.yml run --rm dashboard \
  poly-bot weather station-sync --day 2026-06-04
```

If `poly-bot` on the host says `No such command 'weather'`, reinstall from the repo (`pip install -e ".[production]"`) or always use `docker compose … run` as above.

### Weather-only live loop (markets + catalog + forecasts)

```bash
# Continuous weather-only pipeline (Poly weather markets + sync + aviation forecasts)
docker compose --env-file .env -f docker/docker-compose.prod.yml --profile weather up -d weather-sync
```

## Turn on one extra service

```bash
COMPOSE="docker compose --env-file .env -f docker/docker-compose.prod.yml"

# Refresh catalog from Gamma (periodic — heavy; prefer --once)
$COMPOSE --profile discovery run --rm discovery poly-bot discovery serve --once
$COMPOSE weather sync   # if using weather CLI alias — or:
$COMPOSE run --rm discovery poly-bot weather sync

# Keep discovery running (continuous bandwidth)
$COMPOSE --profile discovery up -d discovery

# REST enrich all expiring + weather bins (many HTTP calls)
$COMPOSE --profile heavy up -d catalog-watch

# WebSocket all markets (highest bandwidth — avoid on Mac)
$COMPOSE --profile heavy up -d ws-daemon

# Telegram + backup + reconcile
$COMPOSE --profile ops up -d alerter
```

## Turn off a service

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml stop ws-daemon
docker compose --env-file .env -f docker/docker-compose.prod.yml stop catalog-watch discovery
```

## Full production (mini PC, not Mac)

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml \
  --profile discovery --profile heavy --profile ops up -d
```

## What uses bandwidth

| Service        | Profile    | Traffic        |
|----------------|------------|----------------|
| dashboard      | (base)     | Low*           |
| postgres       | (base)     | None           |
| discovery      | discovery  | High (Gamma)   |
| catalog-watch  | heavy      | High (CLOB)    |
| ws-daemon      | heavy      | Very high (WS) |
| alerter        | ops        | Low (Telegram) |

\* Weather city pages: REST refresh every 5s for that city only.
