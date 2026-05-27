# poly-bot — Roadmap to trading

Living document. **Trading capital stays off** until every item in
[Phase 1 — Definition of Done](#phase-1--definition-of-done-infra) is checked.

Legend: `[x]` done · `[ ]` todo · `[~]` deferred (post–Phase 1, not a gate)

---

## Phase 1 — Definition of Done (infra)

Everything needed before writing the trading engine.

### Must ship (gates trading)

| # | Item | Status | Notes |
|---|------|--------|-------|
| 1 | Postgres + Alembic + Docker split stack | [x] | discovery, catalog-watch, ws-daemon, dashboard, backup, reconcile, alerter |
| 2 | WS sharding (persistent quotes) | [x] | 7 shards @ 500 tokens |
| 3 | Discovery delta + series on-demand | [x] | 60s delta; series refresh on orphans |
| 4 | Reconcile sidecar + dashboard SSE | [x] | `/api/reconcile`, live dot |
| 5 | Telegram alerter (edge-triggered) | [x] | live since 2026-05-27 |
| 6 | **Fix `delta_stale` false positives** | [ ] | full scan ~12–14 min but threshold 600s → see below |
| 7 | **Runbook** `docs/RUNBOOK.md` | [ ] | only remaining blocker |
| 8 | **Discovery full-scan deadlocks** | [ ] | `PendingRollbackError` on `events` during full scan — real, seen in logs |
| 9 | Pin + roll `0.1.5` image on all services | [ ] | stack still mixed `0.1.4-dev` / digest on some containers |
| 10 | 24h soak: no spurious Telegram pages | [ ] | after #6–#8 |

### Should ship (quality, not hard gates)

| # | Item | Status |
|---|------|--------|
| 11 | Series orphan gap (~600 missing rows) | [~] | Gamma `/series` caps ~50/page |
| 12 | Per-shard WS healthcheck | [~] | quote_age proxy works today |
| 13 | GHCR auto-login on host | [~] | manual `gh auth token \| docker login` |
| 14 | Integration tests (postgres fixture e2e) | [~] | 43 unit tests green |
| 15 | Dashboard per-market SSE push | [~] | table still static after load |
| 16 | Prometheus + Grafana dashboards | [~] | metrics exported, no boards yet |

---

## Known issue: `delta_stale` Telegram noise

**What you see:** FIRING at `delta_age_s=606/643`, RESOLVED at `18/33/45` — always
the same rule, every ~1–2 h.

**Root cause (confirmed in logs):**

1. `last_delta_scan_at` is updated only when a discovery **cycle finishes**.
2. A **full** catalog scan takes **~12–14 minutes** (113k markets + link).
3. Full scans run every **10 minutes** (`DISCOVERY_FULL_SCAN_INTERVAL_MINUTES=10`).
4. Alert threshold is **600 s** → during almost every full scan, age crosses 600
   before the cycle completes → FIRING → scan ends → RESOLVED.

So the pipeline is working; the **metric name is misleading** during full scans.
Occasionally a failed full scan (deadlock on `events`) stretches the gap further.

**Fix options (pick one for #6):**

- **A (recommended):** Alerter uses `discovery_fresh = min(delta_age, full_age)` —
  page only when **both** are stale (discovery truly stuck).
- **B:** Heartbeat — set `last_delta_scan_at` at **start** of full scan too.
- **C:** Raise `ALERT_DELTA_AGE_THRESHOLD` to 1800+ (band-aid).
- **D:** Full scan every 60–120 min; delta stays 60s (architectural best).

---

## Phase 2 — Trading engine (after Phase 1)

Not started. Rough order once infra gate is green:

1. Wallet + CLOB L2 auth (signing, nonces, idempotency)
2. Order model + risk limits (max notional, max open orders, kill switch)
3. Strategy interface (signals in → orders out; paper mode first)
4. Position / PnL tracking + reconciliation vs CLOB
5. Execution telemetry + alerts (`order_rejected`, `fill_lag`, `drawdown`)
6. Paper trading soak → small live capital

---

## Completed work (archive)

<details>
<summary>Blockers 1–5 + polish A (click to expand)</summary>

### 1. WebSocket sharding ✅
- [x] N shards ≤ `catalog_ws_shard_size`, coordinator rebalance
- [~] Per-shard healthcheck

### 2. Series delta scan ✅
- [x] 10 min interval + on-demand orphan refresh (30s cooldown)
- [~] Gamma `/series` pagination cap (~600 orphans remain)

### 3. Alerting ✅
- [x] `alerter` sidecar, 5 rules, Telegram onboarding, 7 unit tests
- [ ] Fix false-positive `delta_stale` (tracked as gate #6 above)

### 4. Scheduled reconcile ✅
- [x] `reconcile-loop` sidecar, `catalog_state` report, Prometheus, `/api/reconcile`

### 5. Runbook — in progress (gate #7)

### A. Dashboard SSE live ✅
- [x] EventSource, live dot, `/api/health`, `/api/reconcile`
- [~] Per-condition table push

</details>

---

## Status quick reference

| Area | State | Notes |
|---|---|---|
| Catalog static (Gamma + DB) | 95% | Postgres + Alembic |
| Taxonomy + classification | 80% | overrides UI; ~600 series orphans |
| Live slice (6h, quotes, OI) | 95% | WS sharded |
| Living DB production | 95% | Docker + backup + reconcile |
| Dashboard | 90% | SSE live |
| Alerting | 85% | transport OK; metric tuning pending (#6) |
| Ops / runbook | 20% | gate #7 |
| **Trading** | **0%** | **gated on Phase 1 table above** |
