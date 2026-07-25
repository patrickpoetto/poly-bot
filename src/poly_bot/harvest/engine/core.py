"""LiveEngine — orchestrates Stage1 monitor, Stage2 watchlist, and executor."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from poly_bot.harvest.engine.executor import OrderExecutor
from poly_bot.harvest.engine.monitor import PriceMonitor
from poly_bot.harvest.engine.reconciler import OrderReconciler
from poly_bot.harvest.engine.state import (
    EngineStats,
    MonitoredMarket,
    infer_duration,
    parse_underlying,
)
from poly_bot.settings import Settings

log = logging.getLogger(__name__)

_MARKET_REFRESH_INTERVAL_S = 30.0  # how often to reload candidates from DB


class LiveEngine:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.stats = EngineStats()
        self._queue: asyncio.Queue[object] = asyncio.Queue(maxsize=100)
        self._cancelled_conditions: set[str] = set()
        self._executor = OrderExecutor(settings, self.stats, cancelled_conditions=self._cancelled_conditions)  # noqa: E501
        self._monitor = PriceMonitor(  # type: ignore[arg-type]
            settings,
            self._queue,
            self.stats,
            get_open=lambda: self._executor.open_positions,
            on_stop_loss=self._executor.close_position,
            cancelled_conditions=self._cancelled_conditions,
        )
        self._reconciler = OrderReconciler(
            settings,
            on_chain_win=self._executor.add_chain_ticket,
            on_order_resolved=self._executor.release_position,
        )

    # ------------------------------------------------------------------ public

    async def run(self) -> None:
        """Start all engine loops.  Blocks until cancelled."""
        mode = "PAPER" if self._settings.paper_trading else "LIVE"
        log.info("LiveEngine starting [mode=%s]", mode)
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._market_refresh_loop(), name="market-refresh")
            tg.create_task(self._monitor.run_stage1(), name="stage1")
            tg.create_task(self._monitor.run_stage2(), name="stage2")
            tg.create_task(self._executor.consume(self._queue), name="executor")  # type: ignore[arg-type]
            tg.create_task(self._reconciler.run(), name="reconciler")

    # ------------------------------------------------------------------ private

    async def _market_refresh_loop(self) -> None:
        while True:
            try:
                markets = await asyncio.get_running_loop().run_in_executor(
                    None, self._load_candidates
                )
                self._monitor.set_markets(markets)
                log.debug("market refresh: %d candidates loaded", len(markets))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("market refresh error: %s", exc)
            await asyncio.sleep(_MARKET_REFRESH_INTERVAL_S)

    @staticmethod
    def _parse_dt(val: Any) -> datetime:
        if isinstance(val, datetime):
            return val.replace(tzinfo=UTC) if val.tzinfo is None else val
        s = str(val).replace("T", " ").split(".")[0].split("+")[0].strip()
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)

    def _load_candidates(self) -> list[MonitoredMarket]:
        """Query DB for active markets in the monitor horizon (single JOIN, no N+1)."""
        from sqlalchemy import text

        from poly_bot.persistence.db import get_engine

        engine = get_engine(self._settings)
        now = datetime.now(UTC)
        crypto_horizon = now + timedelta(minutes=self._settings.engine_monitor_horizon_minutes)
        # Sports lookback: 6h catches in-progress games (baseball/football can run 3-4h).
        # Stage1 handles the resulting token volume via 500-token batches + 30s sweep timeout.
        sports_lookback = now - timedelta(hours=6)
        sports_horizon = now + timedelta(hours=3)

        sql = text("""
            SELECT
                m.condition_id, m.question, m.domain, m.end_ts, m.start_ts,
                t.token_id, t.outcome_label
            FROM markets m
            JOIN tokens t ON t.condition_id = m.condition_id
            WHERE m.active = 1 AND m.closed = 0 AND m.end_ts IS NOT NULL
              AND (
                (m.domain = 'crypto'  AND m.end_ts >= :now  AND m.end_ts <= :crypto_h)
                OR
                (m.domain = 'finance' AND m.end_ts >= :now  AND m.end_ts <= :crypto_h)
                OR
                (m.domain = 'sports'  AND m.end_ts >= :sports_lb AND m.end_ts <= :sports_h)
              )
            ORDER BY m.condition_id
        """)

        def _fmt(dt: datetime) -> str:
            # SQLite stores datetimes as 'YYYY-MM-DD HH:MM:SS' (space, no tz suffix).
            # Python isoformat() produces 'T' + offset which breaks string comparison.
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        with engine.connect() as conn:
            rows = conn.execute(sql, {
                "now": _fmt(now),
                "crypto_h": _fmt(crypto_horizon),
                "sports_lb": _fmt(sports_lookback),
                "sports_h": _fmt(sports_horizon),
            }).fetchall()

        # group tokens by condition_id
        from collections import defaultdict
        by_cid: dict[str, dict[str, Any]] = {}
        tok_by_cid: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for cid, question, domain, end_ts_raw, start_ts_raw, token_id, outcome_label in rows:
            if cid not in by_cid:
                by_cid[cid] = {
                    "question": question, "domain": domain,
                    "end_ts": end_ts_raw, "start_ts": start_ts_raw,
                }
            tok_by_cid[cid].append((token_id, (outcome_label or "").lower()))

        # parse crypto symbol allowlist once
        allowed_symbols: set[str] = set()
        if self._settings.engine_crypto_symbols.strip():
            allowed_symbols = {
                s.strip().lower()
                for s in self._settings.engine_crypto_symbols.split(",")
                if s.strip()
            }

        markets: list[MonitoredMarket] = []
        for cid, meta in by_cid.items():
            tokens = tok_by_cid[cid]
            yes_tok = next((tid for tid, lbl in tokens if lbl in ("yes", "up")), None)
            no_tok = next((tid for tid, lbl in tokens if lbl in ("no", "down")), None)
            if yes_tok is None:
                if tokens:
                    yes_tok = tokens[0][0]
                    no_tok = tokens[1][0] if len(tokens) > 1 else None
                else:
                    continue

            end_ts = self._parse_dt(meta["end_ts"])
            start_ts = self._parse_dt(meta["start_ts"]) if meta["start_ts"] else None
            question_s: str = str(meta["question"] or "")
            domain_s: str = str(meta["domain"] or "")
            underlying = parse_underlying(question_s) if domain_s == "crypto" else None

            # symbol allowlist filter (crypto only)
            if domain_s == "crypto" and allowed_symbols and underlying not in allowed_symbols:
                continue

            markets.append(MonitoredMarket(
                condition_id=cid,
                question=question_s,
                domain=domain_s,
                yes_token_id=yes_tok,
                no_token_id=no_tok,
                end_ts=end_ts,
                start_ts=start_ts,
                duration=infer_duration(start_ts, end_ts),
                underlying=underlying,
            ))
        return markets
