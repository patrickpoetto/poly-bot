"""Application settings loaded from environment / .env."""

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
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
    catalog_quote_hot_hours: float = Field(
        default=2.0,
        validation_alias="CATALOG_QUOTE_HOT_HOURS",
        gt=0,
        le=48,
        description="Harvest + live quote refresh: markets within this many hours of window end",
    )
    catalog_sports_in_play_hours: float = Field(
        default=4.0,
        validation_alias="CATALOG_SPORTS_IN_PLAY_HOURS",
        ge=0.5,
        le=48,
        description="Sports stay in catalog window this long after kickoff (certainty harvest)",
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

    # Certainty harvest (near-resolution buys at 98–99¢)
    harvest_min_net_yield_pct: float = Field(
        default=0.012,
        validation_alias="HARVEST_MIN_NET_YIELD_PCT",
        gt=0,
        le=0.5,
        description="Minimum net return after taker fee (0.012 = 1.2%)",
    )
    harvest_min_buy_price: float = Field(
        default=0.983,
        validation_alias="HARVEST_MIN_BUY_PRICE",
        gt=0.5,
        lt=1.0,
    )
    harvest_max_buy_price: float = Field(
        default=0.995,
        validation_alias="HARVEST_MAX_BUY_PRICE",
        gt=0.5,
        le=1.0,
    )
    harvest_quote_max_age_seconds: float = Field(
        default=180.0,
        validation_alias="HARVEST_QUOTE_MAX_AGE_SECONDS",
        ge=0,
        description="Ignore quotes older than this in harvest scan (0 = no age filter)",
    )
    harvest_max_minutes_left: float = Field(
        default=0.0,
        validation_alias="HARVEST_MAX_MINUTES_LEFT",
        ge=0,
        description="Optional tighter cap than HOT hours (0 = use catalog_quote_hot_hours only)",
    )
    harvest_order_usd: float = Field(
        default=10.0,
        validation_alias="HARVEST_ORDER_USD",
        gt=0,
        le=100_000,
    )
    harvest_min_fill_usd: float = Field(
        default=9.5,
        validation_alias="HARVEST_MIN_FILL_USD",
        ge=0,
        description="Min ask depth (USD) to include in live scan",
    )
    harvest_book_concurrency: int = Field(
        default=20,
        validation_alias="HARVEST_BOOK_CONCURRENCY",
        ge=1,
        le=100,
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

    # Weather trading catalog (stations, daily events, bins)
    weather_forecast_horizon_days: int = Field(
        default=3,
        validation_alias="WEATHER_FORECAST_HORIZON_DAYS",
        ge=0,
        le=14,
        description="Sync today through today+N (0=today only, 3=+3 days)",
    )
    weather_catalog_sync_enabled: bool = Field(
        default=True,
        validation_alias="WEATHER_CATALOG_SYNC_ENABLED",
    )
    weather_market_sync_enabled: bool = Field(
        default=True,
        validation_alias="WEATHER_MARKET_SYNC_ENABLED",
    )
    weather_market_sync_interval_seconds: int = Field(
        default=180,
        validation_alias="WEATHER_MARKET_SYNC_INTERVAL_SECONDS",
        ge=30,
        le=3600,
    )
    weather_aviation_sync_enabled: bool = Field(
        default=True,
        validation_alias="WEATHER_AVIATION_SYNC_ENABLED",
    )
    weather_aviation_interval_seconds: int = Field(
        default=900,
        validation_alias="WEATHER_AVIATION_INTERVAL_SECONDS",
        ge=60,
        le=7200,
    )
    weather_aviation_api_base: str = Field(
        default="https://aviationweather.gov/api/data",
        validation_alias="WEATHER_AVIATION_API_BASE",
    )
    weather_open_meteo_api_base: str = Field(
        default="https://api.open-meteo.com/v1/forecast",
        validation_alias="WEATHER_OPEN_METEO_API_BASE",
    )
    weather_open_meteo_timeout_seconds: float = Field(
        default=20.0,
        validation_alias="WEATHER_OPEN_METEO_TIMEOUT_SECONDS",
        ge=5.0,
        le=90.0,
    )
    weather_open_meteo_models: str = Field(
        default="ecmwf_ifs025,gfs_seamless,icon_seamless",
        validation_alias="WEATHER_OPEN_METEO_MODELS",
    )
    weather_metar_retention_days: int = Field(
        default=14,
        validation_alias="WEATHER_METAR_RETENTION_DAYS",
        ge=3,
        le=90,
    )
    weather_pilot_station_ids: list[str] = Field(
        default_factory=list,
        validation_alias="WEATHER_PILOT_STATION_IDS",
        description="Comma-separated station ids or ICAO with trading_enabled=true",
    )
    weather_wu_sync_enabled: bool = Field(
        default=True,
        validation_alias="WEATHER_WU_SYNC_ENABLED",
    )
    weather_wu_sync_interval_seconds: int = Field(
        default=300,
        validation_alias="WEATHER_WU_SYNC_INTERVAL_SECONDS",
        ge=60,
        le=3600,
        description="Resolution-station WU poll interval (dashboard + serve loop)",
    )
    weather_wu_request_timeout_seconds: float = Field(
        default=20.0,
        validation_alias="WEATHER_WU_REQUEST_TIMEOUT_SECONDS",
        ge=5.0,
        le=60.0,
    )
    weather_wu_concurrency: int = Field(
        default=4,
        validation_alias="WEATHER_WU_CONCURRENCY",
        ge=1,
        le=16,
    )
    weather_wu_api_key: SecretStr | None = Field(
        default=None,
        validation_alias="WEATHER_WU_API_KEY",
        description="Optional api.weather.com key; else scraped from wunderground.com",
    )
    weather_dashboard_poll_seconds: int = Field(
        default=300,
        validation_alias="WEATHER_DASHBOARD_POLL_SECONDS",
        ge=60,
        le=3600,
    )

    @field_validator("weather_pilot_station_ids", mode="before")
    @classmethod
    def _parse_pilot_stations(cls, v: object) -> list[str]:
        if v is None or v == "":
            return []
        if isinstance(v, list):
            return [str(x).strip().lower() for x in v if str(x).strip()]
        return [p.strip().lower() for p in str(v).split(",") if p.strip()]

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
    def catalog_quote_horizon_hours(self) -> float:
        """Lookahead for WS + REST enrich (expiring slice + weather forecast days)."""
        weather_hours = float((self.weather_forecast_horizon_days + 1) * 24)
        return min(168.0, max(self.catalog_expiring_hours, weather_hours))

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
