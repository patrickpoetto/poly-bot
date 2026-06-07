"""Database engine and session management."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, create_engine

from poly_bot.settings import Settings


def get_engine(settings: Settings) -> Engine:
    """Create SQLAlchemy engine (SQLite or PostgreSQL via DATABASE_URL)."""
    url = settings.resolved_database_url
    connect_args: dict[str, object] = {}
    if url.startswith("sqlite"):
        settings.poly_bot_data_dir.mkdir(parents=True, exist_ok=True)
        connect_args = {"check_same_thread": False}

    engine = create_engine(url, connect_args=connect_args, echo=False, pool_pre_ping=True)

    if engine.dialect.name == "sqlite":

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_conn: object, _connection_record: object) -> None:
            from sqlite3 import Connection

            if not isinstance(dbapi_conn, Connection):
                return
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=30000")  # wait up to 30s instead of failing
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.close()

    return engine


def init_db(engine: Engine) -> None:
    """Ensure schema exists — Alembic on Postgres, create_all on SQLite."""
    _register_weather_models()
    if engine.dialect.name == "postgresql":
        run_alembic_upgrade(engine)
        return
    SQLModel.metadata.create_all(engine)
    _migrate_schema(engine)


def _register_weather_models() -> None:
    """Register weather tables on SQLModel metadata before create_all / Alembic."""
    from poly_bot.weather import forecast_schema as _weather_forecast_schema  # noqa: F401
    from poly_bot.weather import schema as _weather_schema  # noqa: F401


def run_alembic_upgrade(engine: Engine) -> None:
    """Apply Alembic migrations to head."""
    from alembic import command
    from alembic.config import Config

    ini_path = _alembic_ini_path()
    cfg = Config(str(ini_path))
    cfg.set_main_option("script_location", str(ini_path.parent / "alembic"))
    cfg.set_main_option("sqlalchemy.url", str(engine.url.render_as_string(hide_password=False)))
    command.upgrade(cfg, "head")


def _alembic_ini_path() -> Path:
    """Locate alembic.ini (repo root or installed package parent)."""
    candidates = [
        Path.cwd() / "alembic.ini",
        Path(__file__).resolve().parents[3] / "alembic.ini",
    ]
    for path in candidates:
        if path.is_file():
            return path
    msg = "alembic.ini not found — run from project root or install with alembic.ini"
    raise FileNotFoundError(msg)


def _migrate_schema(engine: Engine) -> None:
    """Best-effort SQLite migrations for existing catalog DBs."""
    with engine.connect() as conn:
        _add_column_if_missing(conn, "catalog_runs", "series_upserted", "INTEGER DEFAULT 0")
        for table, col, typedef in (
            ("markets", "fees_enabled", "INTEGER DEFAULT 0"),
            ("markets", "fee_type", "TEXT"),
            ("markets", "maker_base_fee", "INTEGER"),
            ("markets", "taker_base_fee", "INTEGER"),
            ("markets", "fee_schedule", "TEXT"),
            ("events", "fees_enabled", "INTEGER DEFAULT 0"),
            ("events", "fee_type", "TEXT"),
            ("events", "maker_base_fee", "INTEGER"),
            ("events", "taker_base_fee", "INTEGER"),
            ("events", "fee_schedule", "TEXT"),
            ("market_enrichment", "maker_base_fee", "INTEGER"),
            ("market_enrichment", "taker_base_fee", "INTEGER"),
            ("markets", "game_start_ts", "TEXT"),
            ("events", "game_start_ts", "TEXT"),
        ):
            _add_column_if_missing(conn, table, col, typedef)
        SQLModel.metadata.create_all(conn)
        conn.commit()


def _add_column_if_missing(conn: object, table: str, column: str, typedef: str) -> None:
    from sqlalchemy.engine import Connection

    if not isinstance(conn, Connection):
        return
    rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    columns = {row[1] for row in rows}
    if column not in columns:
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {typedef}"))


def session_scope(engine: Engine) -> Generator[Session]:
    """Yield a transactional session."""
    with Session(engine) as session:
        yield session
