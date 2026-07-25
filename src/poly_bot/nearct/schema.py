"""SQLModel schema for the NearCT strategy orders.

Orders live in their own SQLite file (data/nearct.db): the catalog DB takes
30s+ write locks during discovery full scans, and losing an order write mid-
resolution crashes the engine loop. Separate file = zero contention.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlmodel import Field, SQLModel

_nearct_engine_cache: dict[str, Any] = {}


def get_nearct_engine(settings: Any) -> Any:
    """Dedicated engine for nearct_orders — never contends with the catalog."""
    from sqlalchemy import create_engine, event

    db_path = settings.poly_bot_data_dir / "nearct.db"
    key = str(db_path.resolve())
    if key in _nearct_engine_cache:
        return _nearct_engine_cache[key]

    engine = create_engine(f"sqlite:///{key}", echo=False)

    @event.listens_for(engine, "connect")
    def _set_pragmas(dbapi_conn: Any, _rec: Any) -> None:
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA busy_timeout=10000")
        cur.close()

    SQLModel.metadata.create_all(engine, tables=[NearctOrder.__table__])  # type: ignore[attr-defined]
    _nearct_engine_cache[key] = engine
    return engine


class NearctOrder(SQLModel, table=True):
    __tablename__ = "nearct_orders"

    id: str = Field(primary_key=True)
    condition_id: str = Field(index=True)
    token_id: str = ""
    question: str = ""
    domain: str = ""
    ask: float = 0.0
    net_yield_pct: float = 0.0
    size_usd: float = 0.0
    # pending | won | lost | stuck (timed out, capital reset)
    # | won_late | lost_late (stuck order resolved later by the reaper)
    status: str = Field(default="pending")
    pnl_usd: float | None = None
    capital_before: float = 0.0
    capital_after: float | None = None
    end_ts: datetime = Field(default_factory=lambda: datetime.now(UTC))
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    resolved_at: datetime | None = None
