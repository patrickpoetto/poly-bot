# poly-bot — Roadmap to trading

Living document. Tick boxes as work lands. Trading capital should not be
deployed until the five blockers below are all green.

## Production blockers (do these before trading)

### 1. WebSocket sharding — `catalog/ws_daemon.py` ✅
- [x] Split a single PersistentWSDaemon into N shards, each holding
      ≤ `catalog_ws_shard_size` (default 500) assets.
- [x] Each shard owns its own websocket, backoff, and resubscribe loop;
      shards write to the same `token_quotes` table.
- [x] Settings: `CATALOG_WS_SHARD_SIZE`, `CATALOG_WS_MAX_SHARDS`.
- [x] Coordinator rebalances every `CATALOG_WS_REFRESH_SECONDS`.
- [ ] Healthcheck per shard (currently any-shard-healthy via quote_age).

### 2. Series delta scan — `discovery/` ✅
- [x] Default refresh interval lowered to 10 min (was 60).
- [x] On-demand refresh: whenever delta scan brings in events whose
      `series_id` is not yet in the series table, trigger a series
      scan with a 30s cool-down.
- [x] Helper `DiscoveryRunner.count_unknown_event_series()` powers
      the trigger and is unit tested.
- [ ] **Follow-up (low priority):** Gamma `/series` appears to cap at
      ~50 items per query regardless of `limit`. After this fix the
      orphan trigger fires correctly but still leaves ~600 event
      `series_id` values without a row in `series`. Investigate
      alternative params (`closed=`, `slug=`, `id_in=…`) or a
      `/series/{id}` per-id fetch path.

### 3. Alerting (paged ops) — code shipped, awaiting onboarding
- [x] Lightweight `alerter` sidecar polls a shared `health_snapshot()`
      helper (`ops/health.py`) and pushes Telegram messages **only on
      state transitions** (open / resolved) — no spam while a condition
      persists.
- [x] Default rules shipped: `db_down`, `quotes_stale`, `delta_stale`,
      `reconcile_stale`, `reconcile_regression`.
- [x] Settings: `ALERTER_ENABLED`, `ALERTER_INTERVAL_SECONDS`,
      `ALERT_QUOTE_AGE_THRESHOLD`, `ALERT_DELTA_AGE_THRESHOLD`,
      `ALERT_RECONCILE_AGE_THRESHOLD`, `TELEGRAM_TOKEN`,
      `TELEGRAM_CHAT_ID`.
- [x] CLI: `poly-bot ops alerter-loop`, `ops telegram-test`,
      `ops telegram-chat-id` (onboarding helper).
- [x] Docker-compose `alerter` service with healthcheck + resource caps.
- [x] README walkthrough: BotFather → token → `/getUpdates` → chat id.
- [x] Unit tests cover rule evaluation, transition edge-triggering,
      and the disabled-by-default safety guard (43 green).
- [x] Image `poly-bot:0.1.5-dev` built; `alerter` container provisioned
      and currently idle (safety: exits cleanly when `ALERTER_ENABLED=false`).
- [ ] **Manual step pending:** paste `TELEGRAM_TOKEN` into `.env`,
      then run `docker compose ... run --rm alerter poly-bot ops
      telegram-chat-id` to capture `TELEGRAM_CHAT_ID`, then flip
      `ALERTER_ENABLED=true` and restart `alerter`.
- [ ] Smoke test in production: force `quote_age` over threshold, confirm
      FIRING/RESOLVED pair lands in Telegram.

### 4. Scheduled reconcile service ✅
- [x] New `poly-bot ops reconcile-loop` command + `reconcile` sidecar
      in docker-compose, interval via `RECONCILE_INTERVAL_SECONDS`.
- [x] Each run persists `last_reconcile_at` + the JSON report to
      `catalog_state`; exposed via `/api/reconcile`.
- [x] Prometheus gauges `polybot_reconcile_issues{metric=...}` and
      counter `polybot_reconcile_runs_total{outcome=...}`.
- [x] Unit tests assert state and counts after one run.

### 5. Runbook (`docs/RUNBOOK.md`)
- [ ] Symptoms → diagnosis → fix for each known incident class:
  - catalog-watch stuck (deadlock or hung await)
  - postgres disk pressure
  - GHCR pull 401
  - WS reconnect storm
  - backup volume full
  - migration failure
- [ ] One-line restore-from-backup procedure.

## Polish (parallelizable, not blockers)

### A. Dashboard SSE live ✅
- [x] `<meta http-equiv="refresh">` gone — page renders once.
- [x] Vanilla JS `EventSource` consumer with auto-reconnect.
- [x] Live indicator dot (green pulse / amber lag / red stale).
- [x] Stats counters and quote/delta age tick every 5s via
      `/api/stream`, fed by new `_live_snapshot` helper.
- [x] New `/api/health` + `/api/reconcile` JSON endpoints reuse
      the same snapshot.
- [ ] Per-condition push of new markets entering the window
      (table is still server-rendered on initial load).

### B. GHCR auto-login on the host
- [ ] Today we run `gh auth token | docker login ghcr.io` by hand.
- [ ] Either make the package public or write a small launchd /
      systemd unit that refreshes the login.

### C. Integration tests
- [ ] Today only unit tests in `tests/` (43 green).
- [ ] Add end-to-end: spin postgres in a fixture, run a full
      discovery → watch → enrich cycle against recorded Gamma payloads.

## Status quick reference

| Area | State | Notes |
|---|---|---|
| Catalog static (Gamma + DB) | 95% | Postgres + Alembic shipped |
| Taxonomy + classification | 80% | overrides UI live, batch reclassify |
| Live slice (6h window, quotes, OI) | 95% | WS sharded across 7 sockets |
| Living DB production | 95% | Docker split + healthchecks + backup + reconcile |
| Dashboard | 90% | SSE live indicator + JSON endpoints |
| Alerting | 90% | code shipped — token + chat_id + smoke pending |
| Trading | 0% | gated on blocker 5 (runbook) |
