"""Database backup helpers."""

from __future__ import annotations

import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from poly_bot.settings import Settings


def backup_catalog(settings: Settings, *, dest_dir: Path | None = None) -> Path:
    """Backup SQLite file or invoke pg_dump for PostgreSQL."""
    out_dir = dest_dir or (settings.poly_bot_data_dir / "backups")
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    if settings.uses_postgres:
        out_path = out_dir / f"catalog_{stamp}.sql"
        result = subprocess.run(  # noqa: S603
            ["pg_dump", settings.resolved_database_url],
            check=True,
            capture_output=True,
            text=True,
        )
        out_path.write_text(result.stdout, encoding="utf-8")
        return out_path

    src = settings.catalog_db_path
    if not src.exists():
        msg = f"SQLite catalog not found: {src}"
        raise FileNotFoundError(msg)
    out_path = out_dir / f"catalog_{stamp}.db"
    shutil.copy2(src, out_path)
    wal = Path(str(src) + "-wal")
    if wal.exists():
        shutil.copy2(wal, out_dir / f"catalog_{stamp}.db-wal")
    return out_path
