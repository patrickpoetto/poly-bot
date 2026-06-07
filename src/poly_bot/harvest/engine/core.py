"""LiveEngine — orchestrates Stage1 monitor, Stage2 watchlist, and executor."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from poly_bot.harvest.engine.executor import OrderExecutor
from poly_bot.harvest.engine.monitor import PriceMonitor
from poly_bot.harvest.engine.state import (
    EngineStats,
    MarketDuration,
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
        self._monitor = PriceMonitor(settings, self._queue, self.stats)  # type: ignore[arg-type]
        self._executor = OrderExecutor(settings, self.stats)

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

    def _load_candidates(self) -> list[MonitoredMarket]:
        """Query DB for active markets in the monitor horizon."""
        from sqlmodel import Session, select
        from poly_bot.persistence.db import get_engine
        from poly_bot.persistence.schema import Market, Token

        engine = get_engine(self._settings)
        horizon = datetime.now(UTC) + timedelta(
            minutes=self._settings.engine_monitor_horizon_minutes
        )
        markets: list[MonitoredMarket] = []
        with Session(engine) as session:
            stmt = (
                select(Market)
                .where(
                    Market.active.is_(True),
                    Market.closed.is_(False),
                    Market.end_ts.isnot(None),
                    Market.end_ts >= datetime.now(UTC),
                    Market.end_ts <= horizon,
                    Market.domain.in_(["crypto", "sports"]),
                )
            )
            db_markets = session.exec(stmt).all()
            for m in db_markets:
                if m.end_ts is None:
                    continue
                # resolve UTC-aware end_ts
                end_ts = m.end_ts
                if end_ts.tzinfo is None:
                    end_ts = end_ts.replace(tzinfo=UTC)
                start_ts = m.start_ts
                if start_ts is not None and start_ts.tzinfo is None:
                    start_ts = start_ts.replace(tzinfo=UTC)
                duration = infer_duration(start_ts, end_ts)
                underlying = parse_underlying(m.question) if m.domain == "crypto" else None

                # fetch tokens
                tokens = session.exec(
                    select(Token).where(Token.condition_id == m.condition_id)
                ).all()
                yes_tok = next(
                    (t.token_id for t in tokens if t.outcome_label.lower() in ("yes", "up")),
                    None,
                )
                no_tok = next(
                    (t.token_id for t in tokens if t.outcome_label.lower() in ("no", "down")),
                    None,
                )
                if yes_tok is None:
                    # fall back to first token
                    if tokens:
                        yes_tok = tokens[0].token_id
                        no_tok = tokens[1].token_id if len(tokens) > 1 else None
                    else:
                        continue

                markets.append(
                    MonitoredMarket(
                        condition_id=m.condition_id,
                        question=m.question,
                        domain=m.domain,
                        yes_token_id=yes_tok,
                        no_token_id=no_tok,
                        end_ts=end_ts,
                        start_ts=start_ts,
                        duration=duration,
                        underlying=underlying,
                    )
                )
        return markets
