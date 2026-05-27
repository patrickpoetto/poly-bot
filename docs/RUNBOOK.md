# poly-bot — Operations Runbook

Quick reference for production incidents. Stack: Docker Compose
(`docker/docker-compose.prod.yml`), Postgres 16, services on the `backend`
network.

## Service map

| Service | Command | Health signal |
|---------|---------|---------------|
| `postgres` | — | `pg_isready` |
| `migrate` | one-shot Alembic | exits 0 |
| `discovery` | `discovery serve` | `last_discovery_activity_at` < 15 min |
| `catalog-watch` | `catalog watch` | `quote_age_s` < 600 |
| `ws-daemon` | `catalog ws-daemon` | `quote_age_s` < 300 |
| `dashboard` | `catalog dashboard` | HTTP `/health` |
| `backup` | `ops backup-loop` | logs `backup_complete` |
| `reconcile` | `ops reconcile-loop` | `reconcile_age_s` < 4 h |
| `alerter` | `ops alerter-loop` | Telegram (no self-health) |

## Daily checks (2 min)

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml ps
curl -s http://127.0.0.1:8765/api/health | python -m json.tool
```

All containers should be `Up` and `(healthy)` where a healthcheck exists.

---

## Incident: `discovery_stale` / `delta_stale` Telegram alert

**Symptoms:** `discovery_activity_age_s` or legacy `delta_age_s` FIRING.

**Diagnosis:**

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml logs discovery --tail 80
```

| Log pattern | Meaning |
|-------------|---------|
| `discovery_cycle_full` + `scan_progress` | Normal — full scan ~12–14 min |
| `discovery_scheduler_cycle_failed` + `deadlock` | Transient — should retry (see below) |
| No logs for > 15 min | Discovery stuck or crashed |

**Fix:**

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml restart discovery
```

If deadlocks repeat: ensure image ≥ `0.1.5-dev` (deadlock retry + heartbeat).
Tune `DISCOVERY_FULL_SCAN_INTERVAL_MINUTES=30` so full scans do not overlap
delta cadence.

---

## Incident: `quotes_stale` Telegram alert

**Symptoms:** `quote_age_s` FIRING; dashboard live dot red.

**Diagnosis:**

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml logs ws-daemon --tail 50
docker compose --env-file .env -f docker/docker-compose.prod.yml logs catalog-watch --tail 30
```

| Cause | Fix |
|-------|-----|
| WS reconnect storm | `restart ws-daemon`; check Polymarket status |
| `catalog-watch` hung | `restart catalog-watch` (watchdog logs `cycle_timeout`) |
| Postgres down | Fix postgres first |

---

## Incident: catalog-watch stuck (deadlock / hung cycle)

**Symptoms:** No `catalog_watch_cycle` logs; quotes age climbing; container
still `healthy` until quote threshold exceeded.

**Diagnosis:**

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml logs catalog-watch --tail 100 | grep -E "cycle|deadlock|timeout|error"
```

**Fix:**

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml restart catalog-watch
```

Persistent deadlocks: reduce `CATALOG_OI_CONCURRENCY` or stagger discovery
full scans (`DISCOVERY_FULL_SCAN_INTERVAL_MINUTES=30`).

---

## Incident: discovery full-scan deadlock

**Symptoms:** `discovery_scheduler_cycle_failed`, `PendingRollbackError` or
`DeadlockDetected` on `events` / `markets`.

**Fix (automatic in ≥ 0.1.5-dev):** runner retries up to 3× with backoff;
per-record `session.rollback()` prevents poisoned sessions.

**Manual:**

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml restart discovery
```

---

## Incident: Postgres disk pressure

**Symptoms:** writes fail; containers restart; `db_down` Telegram alert.

**Diagnosis:**

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml exec postgres \
  psql -U poly_bot -d poly_bot -c "SELECT pg_size_pretty(pg_database_size('poly_bot'));"
docker system df
```

**Fix:** prune old backups on `pg_backups` volume; `VACUUM` during low traffic;
expand disk; reduce `DISCOVERY_CLOSED_LOOKBACK_DAYS` if bootstrapping closed
markets is too heavy.

---

## Incident: GHCR pull 401

**Symptoms:** `pull access denied` / `401 Unauthorized` on `docker compose pull`.

**Fix:**

```bash
gh auth token | docker login ghcr.io -u USERNAME --password-stdin
# or make the package public in GitHub → Packages → Package settings
```

Set `POLY_BOT_IMAGE=ghcr.io/<owner>/poly-bot:<tag>` in `.env`.

---

## Incident: WS reconnect storm

**Symptoms:** `ws-daemon` logs `no close frame` / constant reconnect; quote_age
spiky.

**Fix:**

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml restart ws-daemon
```

Verify `CATALOG_WS_SHARD_SIZE` (default 500) and shard count in logs.
Increase `ping_interval` only if upstream allows (already 30s in code).

---

## Incident: backup volume full

**Symptoms:** `backup` container errors; disk full on host.

**Diagnosis:**

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml exec backup ls -lh /backups
```

**Fix:** lower `BACKUP_RETENTION_DAYS`; delete oldest dumps manually; expand
volume.

---

## Incident: migration failure

**Symptoms:** `migrate` service exits non-zero; app containers never start.

**Diagnosis:**

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml logs migrate
```

**Fix:** resolve Alembic conflict (often duplicate index); run manually:

```bash
docker compose --env-file .env -f docker/docker-compose.prod.yml run --rm migrate \
  poly-bot ops migrate
```

Never skip hooks or force-push schema without a backup.

---

## Restore from backup

**One-liner** (adjust timestamp):

```bash
# stop writers
docker compose --env-file .env -f docker/docker-compose.prod.yml stop discovery catalog-watch ws-daemon reconcile alerter

# restore (example — pick latest dump from volume)
docker compose --env-file .env -f docker/docker-compose.prod.yml exec -T postgres \
  pg_restore -U poly_bot -d poly_bot --clean --if-exists < /path/to/backup.dump

# bring stack back
docker compose --env-file .env -f docker/docker-compose.prod.yml up -d
```

SQLite dev: copy `data/backups/catalog-*.db` over `data/catalog.db`.

---

## Deploy / upgrade

```bash
# from repo root
docker build -f docker/Dockerfile -t poly-bot:0.1.5-dev .
# update POLY_BOT_IMAGE in .env
docker compose --env-file .env -f docker/docker-compose.prod.yml up -d --force-recreate
docker compose --env-file .env -f docker/docker-compose.prod.yml ps
```

---

## Telegram alerter rules

| Rule | Fires when |
|------|------------|
| `db_down` | Postgres unreachable |
| `quotes_stale` | `quote_age_s > ALERT_QUOTE_AGE_THRESHOLD` (600) |
| `discovery_stale` | `discovery_activity_age_s > ALERT_DELTA_AGE_THRESHOLD` (600) |
| `reconcile_stale` | no reconcile in 4 h |
| `reconcile_regression` | issue count increased vs last run |

Messages are **edge-triggered** (FIRING once on open, RESOLVED once on clear).

Onboarding: see README → “Telegram alerter setup”.
