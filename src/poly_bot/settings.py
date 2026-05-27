"""Application settings loaded from environment / .env."""

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration — 12-factor, validated at startup."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application
    poly_bot_env: Literal["development", "staging", "production"] = Field(
        default="development",
        validation_alias="POLY_BOT_ENV",
    )
    poly_bot_log_level: str = Field(default="INFO", validation_alias="POLY_BOT_LOG_LEVEL")
    poly_bot_log_json: bool = Field(default=False, validation_alias="POLY_BOT_LOG_JSON")
    poly_bot_data_dir: Path = Field(default=Path("./data"), validation_alias="POLY_BOT_DATA_DIR")
    database_url: str | None = Field(default=None, validation_alias="DATABASE_URL")

    # Polymarket API hosts
    polymarket_gamma_host: str = Field(
        default="https://gamma-api.polymarket.com",
        validation_alias="POLYMARKET_GAMMA_HOST",
    )
    polymarket_data_host: str = Field(
        default="https://data-api.polymarket.com",
        validation_alias="POLYMARKET_DATA_HOST",
    )
    polymarket_clob_host: str = Field(
        default="https://clob.polymarket.com",
        validation_alias="POLYMARKET_CLOB_HOST",
    )
    polymarket_ws_market_url: str = Field(
        default="wss://ws-subscriptions-clob.polymarket.com/ws/market",
        validation_alias="POLYMARKET_WS_MARKET_URL",
    )

    # Discovery
    discovery_closed_lookback_days: int = Field(
        default=7,
        validation_alias="DISCOVERY_CLOSED_LOOKBACK_DAYS",
        ge=0,
        le=90,
    )
    discovery_full_scan_interval_minutes: int = Field(
        default=30,
        validation_alias="DISCOVERY_FULL_SCAN_INTERVAL_MINUTES",
        ge=1,
    )
    discovery_delta_scan_interval_seconds: int = Field(
        default=60,
        validation_alias="DISCOVERY_DELTA_SCAN_INTERVAL_SECONDS",
        ge=10,
    )
    discovery_http_concurrency: int = Field(
        default=10,
        validation_alias="DISCOVERY_HTTP_CONCURRENCY",
        ge=1,
        le=50,
    )
    discovery_request_timeout_seconds: float = Field(
        default=30.0,
        validation_alias="DISCOVERY_REQUEST_TIMEOUT_SECONDS",
        gt=0,
    )
    discovery_series_refresh_interval_minutes: int = Field(
        default=10,
        validation_alias="DISCOVERY_SERIES_REFRESH_INTERVAL_MINUTES",
        ge=2,
    )

    # Live catalog (expiring window + quotes)
    catalog_expiring_hours: float = Field(
        default=6.0,
        validation_alias="CATALOG_EXPIRING_HOURS",
        gt=0,
        le=168,
    )
    catalog_enrich_interval_seconds: float = Field(
        default=60.0,
        validation_alias="CATALOG_ENRICH_INTERVAL_SECONDS",
        ge=5,
    )
    catalog_ws_enabled: bool = Field(default=True, validation_alias="CATALOG_WS_ENABLED")
    catalog_ws_stale_seconds: float = Field(
        default=5.0,
        validation_alias="CATALOG_WS_STALE_SECONDS",
        gt=0,
    )
    catalog_ws_persistent: bool = Field(
        default=True,
        validation_alias="CATALOG_WS_PERSISTENT",
    )
    catalog_ws_refresh_seconds: float = Field(
        default=120.0,
        validation_alias="CATALOG_WS_REFRESH_SECONDS",
        ge=15.0,
    )
    catalog_ws_shard_size: int = Field(
        default=500,
        validation_alias="CATALOG_WS_SHARD_SIZE",
        ge=50,
        le=2000,
    )
    catalog_ws_max_shards: int = Field(
        default=16,
        validation_alias="CATALOG_WS_MAX_SHARDS",
        ge=1,
        le=64,
    )
    catalog_fetch_open_interest: bool = Field(
        default=True,
        validation_alias="CATALOG_FETCH_OI",
    )
    catalog_oi_concurrency: int = Field(
        default=50,
        validation_alias="CATALOG_OI_CONCURRENCY",
        ge=1,
        le=100,
    )
    catalog_enrich_only_stale: bool = Field(
        default=True,
        validation_alias="CATALOG_ENRICH_ONLY_STALE",
    )
    catalog_enrich_stale_seconds: float = Field(
        default=300.0,
        validation_alias="CATALOG_ENRICH_STALE_SECONDS",
        ge=5,
    )
    catalog_dashboard_host: str = Field(
        default="127.0.0.1",
        validation_alias="CATALOG_DASHBOARD_HOST",
    )
    catalog_dashboard_port: int = Field(
        default=8765,
        validation_alias="CATALOG_DASHBOARD_PORT",
        ge=1024,
        le=65535,
    )
    metrics_enabled: bool = Field(default=False, validation_alias="METRICS_ENABLED")
    metrics_port: int = Field(
        default=9090,
        validation_alias="METRICS_PORT",
        ge=1024,
        le=65535,
    )

    # Telegram alerter
    telegram_token: SecretStr | None = Field(
        default=None,
        validation_alias="TELEGRAM_TOKEN",
    )
    telegram_chat_id: str | None = Field(default=None, validation_alias="TELEGRAM_CHAT_ID")
    alerter_enabled: bool = Field(default=False, validation_alias="ALERTER_ENABLED")
    alerter_interval_seconds: int = Field(
        default=60,
        validation_alias="ALERTER_INTERVAL_SECONDS",
        ge=10,
    )
    alert_quote_age_threshold: int = Field(
        default=600,
        validation_alias="ALERT_QUOTE_AGE_THRESHOLD",
        ge=30,
    )
    alert_delta_age_threshold: int = Field(
        default=600,
        validation_alias="ALERT_DELTA_AGE_THRESHOLD",
        ge=30,
    )
    alert_reconcile_age_threshold: int = Field(
        default=14400,
        validation_alias="ALERT_RECONCILE_AGE_THRESHOLD",
        ge=300,
    )

    # Wallet (optional until trading)
    private_key: SecretStr | None = Field(default=None, validation_alias="PRIVATE_KEY")
    wallet_address: str | None = Field(default=None, validation_alias="WALLET_ADDRESS")
    chain_id: int = Field(default=137, validation_alias="CHAIN_ID")
    rpc_url: str = Field(default="https://polygon-rpc.com", validation_alias="RPC_URL")

    @property
    def catalog_db_path(self) -> Path:
        return self.poly_bot_data_dir / "catalog.db"

    @property
    def resolved_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        return f"sqlite:///{self.catalog_db_path.resolve()}"

    @property
    def uses_postgres(self) -> bool:
        return bool(self.database_url and self.database_url.startswith("postgresql"))

    @property
    def is_production(self) -> bool:
        return self.poly_bot_env == "production"


def get_settings() -> Settings:
    """Load settings once per process (cheap; re-reads .env each call)."""
    return Settings()
