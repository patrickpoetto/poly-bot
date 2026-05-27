"""Telegram alerter — polls the health snapshot and pages on state changes.

Design constraints:
- *Edge-triggered*: a rule fires only when its state transitions from
  ``ok`` → ``firing`` (and again when it goes back to ``ok``). No
  recurring spam while the same condition persists.
- *Stateless across restarts is acceptable*: a restart will re-page open
  alerts once, which is desirable (acts as a heartbeat after deploy).
- *No external state store*: alert states live in-memory.
- *Best-effort delivery*: Telegram errors are logged and swallowed; the
  loop never crashes the container.
"""

from __future__ import annotations

import asyncio
import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable

import structlog

from poly_bot.notifier.telegram import TelegramClient
from poly_bot.ops.health import health_snapshot
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class Rule:
    name: str
    description: str
    evaluate: Callable[[dict[str, Any], "AlerterContext"], tuple[bool, str]]


@dataclass
class AlerterContext:
    """Per-process state visible to rules (last reconcile fingerprint, …)."""

    last_reconcile_issue_counts: dict[str, int] | None = None


def default_rules(settings: Settings) -> list[Rule]:
    quote_max = settings.alert_quote_age_threshold
    delta_max = settings.alert_delta_age_threshold
    recon_max = settings.alert_reconcile_age_threshold

    def _quote_stale(snap: dict[str, Any], _ctx: AlerterContext) -> tuple[bool, str]:
        age = snap.get("quote_age_s")
        if not isinstance(age, int):
            return False, "no quote data yet"
        return age > quote_max, f"quote_age_s={age} (> {quote_max})"

    def _discovery_stale(snap: dict[str, Any], _ctx: AlerterContext) -> tuple[bool, str]:
        """Uses heartbeat written at cycle *start*, not delta completion time."""
        age = snap.get("discovery_activity_age_s")
        if not isinstance(age, int):
            return False, "no discovery activity yet"
        return age > delta_max, f"discovery_activity_age_s={age} (> {delta_max})"

    def _reconcile_stale(snap: dict[str, Any], _ctx: AlerterContext) -> tuple[bool, str]:
        age = snap.get("reconcile_age_s")
        if not isinstance(age, int):
            return False, "no reconcile run yet"
        return age > recon_max, f"reconcile_age_s={age} (> {recon_max})"

    def _reconcile_regression(
        snap: dict[str, Any], ctx: AlerterContext
    ) -> tuple[bool, str]:
        report = snap.get("reconcile_report")
        if not isinstance(report, dict):
            return False, "no reconcile report"
        issues = report.get("issues")
        if not isinstance(issues, dict):
            return False, "report has no issues key"
        counts = {k: int(v) for k, v in issues.items() if isinstance(v, (int, float))}
        prev = ctx.last_reconcile_issue_counts
        ctx.last_reconcile_issue_counts = counts
        if prev is None:
            return False, "first observation, baseline only"
        regressions = {
            k: (prev.get(k, 0), v) for k, v in counts.items() if v > prev.get(k, 0)
        }
        if not regressions:
            return False, "no regression"
        summary = ", ".join(f"{k}: {a}→{b}" for k, (a, b) in regressions.items())
        return True, f"new issues — {summary}"

    def _db_down(snap: dict[str, Any], _ctx: AlerterContext) -> tuple[bool, str]:
        status = snap.get("db")
        if isinstance(status, str) and status != "ok":
            return True, status
        return False, "db ok"

    return [
        Rule("db_down", "Postgres unreachable", _db_down),
        Rule("quotes_stale", f"No quotes for > {quote_max}s", _quote_stale),
        Rule(
            "discovery_stale",
            f"No discovery activity for > {delta_max}s",
            _discovery_stale,
        ),
        Rule(
            "reconcile_stale",
            f"No reconcile run for > {recon_max}s",
            _reconcile_stale,
        ),
        Rule(
            "reconcile_regression",
            "Reconcile reported a new integrity issue",
            _reconcile_regression,
        ),
    ]


def evaluate(
    rules: list[Rule], snapshot: dict[str, Any], ctx: AlerterContext
) -> dict[str, tuple[bool, str]]:
    """Run all rules against the snapshot; return ``{name: (firing, detail)}``."""
    out: dict[str, tuple[bool, str]] = {}
    for rule in rules:
        try:
            out[rule.name] = rule.evaluate(snapshot, ctx)
        except Exception as exc:
            logger.exception("alerter_rule_error", rule=rule.name)
            out[rule.name] = (False, f"rule error: {exc.__class__.__name__}")
    return out


def transitions(
    previous: dict[str, bool], current: dict[str, tuple[bool, str]]
) -> list[tuple[str, str, bool, str]]:
    """Return [(name, kind, firing, detail)] for rules that changed state.

    ``kind`` is ``"open"`` when ok → firing and ``"resolved"`` otherwise.
    """
    out: list[tuple[str, str, bool, str]] = []
    for name, (firing, detail) in current.items():
        was = previous.get(name, False)
        if firing == was:
            continue
        out.append((name, "open" if firing else "resolved", firing, detail))
    return out


def render_message(name: str, kind: str, detail: str, *, host: str) -> str:
    icon = "🔴" if kind == "open" else "✅"
    headline = "FIRING" if kind == "open" else "RESOLVED"
    ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    return (
        f"{icon} *poly-bot* — `{name}` {headline}\n"
        f"`{detail}`\n"
        f"_host:_ `{host}`  _at:_ `{ts}`"
    )


async def run_alerter_loop(
    settings: Settings,
    *,
    snapshot_fn: Callable[[Settings], dict[str, Any]] | None = None,
    sender: Callable[[str], Awaitable[bool]] | None = None,
    rules: list[Rule] | None = None,
    iterations: int | None = None,
) -> None:
    """Main alerter loop. ``iterations`` lets tests run a bounded number of cycles."""

    if not settings.alerter_enabled:
        logger.warning("alerter_disabled — set ALERTER_ENABLED=1 to enable")
        return
    if sender is None:
        token = settings.telegram_token.get_secret_value() if settings.telegram_token else None
        chat_id = settings.telegram_chat_id
        if not token or not chat_id:
            logger.error("alerter_missing_credentials")
            return
        client = TelegramClient(token, chat_id=chat_id)
        sender = client.send_message

    snapshot_fn = snapshot_fn or health_snapshot
    rules = rules or default_rules(settings)
    ctx = AlerterContext()
    host = socket.gethostname()
    previous: dict[str, bool] = {}
    interval = settings.alerter_interval_seconds

    logger.info(
        "alerter_started",
        interval_s=interval,
        rules=[r.name for r in rules],
    )

    i = 0
    while True:
        try:
            snapshot = snapshot_fn(settings)
            results = evaluate(rules, snapshot, ctx)
            changes = transitions(previous, results)
            for name, kind, _firing, detail in changes:
                message = render_message(name, kind, detail, host=host)
                ok = await sender(message)
                logger.info(
                    "alerter_paged",
                    rule=name,
                    kind=kind,
                    detail=detail,
                    delivered=ok,
                )
            previous = {name: firing for name, (firing, _) in results.items()}
        except Exception:
            logger.exception("alerter_cycle_error")
        i += 1
        if iterations is not None and i >= iterations:
            return
        await asyncio.sleep(interval)
