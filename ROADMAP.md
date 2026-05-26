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

### 3. Alerting (paged ops)
- [ ] Telegram webhook on `quote_age_s > 600`, `catalog_watch_cycle_timeout`,
      `discovery_scan_failed`, `migrate` failed.
- [ ] Prometheus → Alertmanager → Telegram (or simpler: directly from
      ops health command on a cron).
- [ ] Document in README how to set `TELEGRAM_TOKEN` / `TELEGRAM_CHAT_ID`.
- **Why blocker:** without paging you don't know it broke until you look.

### 4. Scheduled reconcile service
- [ ] `poly-bot ops reconcile` exists but is manual.
- [ ] Add a `reconcile` service to docker-compose, looping every N hours.
- [ ] Surface anomalies (null fee_type, empty raw_payload, orphan
      condition_id) on the dashboard or via alert.

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

### A. Dashboard SSE live
- [ ] Replace HTML auto-refresh with the existing `/api/stream` SSE
      endpoint on the React/HTMX side.
- [ ] Add a "live" indicator and per-condition push.

### B. GHCR auto-login on the host
- [ ] Today we run `gh auth token | docker login ghcr.io` by hand.
- [ ] Either make the package public or write a small launchd /
      systemd unit that refreshes the login.

### C. Integration tests
- [ ] Today only unit tests in `tests/` (23 green).
- [ ] Add end-to-end: spin postgres in a fixture, run a full
      discovery → watch → enrich cycle against recorded Gamma payloads.

## Status quick reference

| Area | State | Notes |
|---|---|---|
| Catalog static (Gamma + DB) | 95% | Postgres + Alembic shipped |
| Taxonomy + classification | 80% | overrides UI live, batch reclassify |
| Live slice (6h window, quotes, OI) | 85% | WS sharding pending |
| Living DB production | 90% | Docker split + healthchecks + backup |
| Trading | 0% | gated on blockers 1, 3, 4 |
