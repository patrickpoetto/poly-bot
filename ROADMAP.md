# poly-bot — Roadmap to trading

Living document. **Trading capital stays off** until every item in
[Phase 1 — Definition of Done](#phase-1--definition-of-done-infra) is checked.

Legend: `[x]` done · `[ ]` todo · `[~]` deferred (post–Phase 1, not a gate)

---

## Phase 1 — Definition of Done (infra)

### Must ship (gates trading)

| # | Item | Status | Notes |
|---|------|--------|-------|
| 1 | Postgres + Alembic + Docker split stack | [x] | 8 services on compose |
| 2 | WS sharding (persistent quotes) | [x] | 7 shards @ 500 tokens |
| 3 | Discovery delta + series on-demand | [x] | 60s delta; series refresh on orphans |
| 4 | Reconcile sidecar + dashboard SSE | [x] | `/api/reconcile`, live dot |
| 5 | Telegram alerter (edge-triggered) | [x] | live since 2026-05-27 |
| 6 | Fix `delta_stale` false positives | [x] | `discovery_stale` + `last_discovery_activity_at` heartbeat |
| 7 | Runbook `docs/RUNBOOK.md` | [x] | incidents + restore + deploy |
| 8 | Discovery full-scan deadlocks | [x] | retry 3×, `no_autoflush`, per-record rollback |
| 9 | Pin + roll `0.1.5-dev` on all services | [x] | force-recreate 2026-05-27 |
| 10 | 24h soak: no spurious Telegram pages | [ ] | **your watch** — started after #6–#9 deploy |

**Phase 1 = complete when #10 is checked** (expected: no `discovery_stale`
FIRING/RESOLVED pairs every hour during normal full scans).

### Should ship (quality, not hard gates)

| # | Item | Status |
|---|------|--------|
| 11 | Series orphan gap (~600 missing rows) | [~] | Gamma `/series` caps ~50/page |
| 12 | Per-shard WS healthcheck | [~] | quote_age proxy works today |
| 13 | GHCR auto-login on host | [~] | manual `gh auth token \| docker login` |
| 14 | Integration tests (postgres fixture e2e) | [~] | 44 unit tests green |
| 15 | Dashboard per-market SSE push | [x] | Yes/No/Spread tick every 5s via SSE |
| 16 | Prometheus + Grafana dashboards | [~] | metrics exported, no boards yet |

---

## Phase 2 — Trading engine (next)

1. Wallet + CLOB L2 auth (signing, nonces, idempotency)
2. Order model + risk limits (max notional, kill switch)
3. Strategy interface (signals in → orders out; paper mode first)
4. Position / PnL tracking + reconciliation vs CLOB
5. Execution telemetry + alerts
6. Paper trading soak → small live capital

---

## Changelog (infra phase)

| Date | Change |
|------|--------|
| 2026-05-27 | Phase 1 gates #6–#9: discovery heartbeat alerter rule, deadlock retry, runbook, `0.1.5-dev` roll |
| 2026-05-27 | Telegram alerter wired (`discovery_stale` replaces noisy `delta_stale`) |
| 2026-05-26 | Reconcile sidecar + dashboard SSE live |
| 2026-05-26 | WS sharding + series on-demand refresh |

---

## Status quick reference

| Area | State |
|---|---|
| Catalog static | 95% |
| Taxonomy | 80% |
| Live slice | 95% |
| Living DB production | 98% |
| Dashboard | 90% |
| Alerting | 98% (soak #10 pending) |
| Ops / runbook | 100% |
| **Trading** | **0% — start after #10** |
