"""Pytest fixtures."""

import pytest

from poly_bot.settings import Settings


@pytest.fixture
def settings(tmp_path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Isolated data dir — never touch project ``data/catalog.db``."""
    data_dir = tmp_path / "data"
    monkeypatch.setenv("POLY_BOT_DATA_DIR", str(data_dir))
    return Settings(
        poly_bot_data_dir=data_dir,
        poly_bot_log_json=True,
        discovery_http_concurrency=2,
    )
