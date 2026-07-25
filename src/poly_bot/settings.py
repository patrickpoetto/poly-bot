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
    catalog_quote_hot_hours: float = Field(
        default=2.0,
        validation_alias="CATALOG_QUOTE_HOT_HOURS",
        gt=0,
        le=48,
        description=(
            "HOT tier: non-sports = hours to window end; "
            "sports in-play = from kickoff; sports pre-match = hours to kickoff"
        ),
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

    # Certainty harvest (near-resolution buys at 98-99c)
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
    harvest_max_bid_ask_spread: float = Field(
        default=0.0,
        validation_alias="HARVEST_MAX_BID_ASK_SPREAD",
        ge=0,
        le=0.5,
        description="Live scan: max ask-bid on buy side for non-sports (0 = off)",
    )
    harvest_sports_max_bid_ask_spread: float = Field(
        default=0.08,
        validation_alias="HARVEST_SPORTS_MAX_BID_ASK_SPREAD",
        ge=0,
        le=0.5,
        description="Live scan: max ask-bid for sports only (0 = complement check only)",
    )
    harvest_max_complement_sum: float = Field(
        default=1.02,
        validation_alias="HARVEST_MAX_COMPLEMENT_SUM",
        ge=0,
        le=2.0,
        description="Live scan: reject if ask + other_bid exceeds this (stale loser asks)",
    )
    harvest_max_book_age_seconds: float = Field(
        default=300.0,
        validation_alias="HARVEST_MAX_BOOK_AGE_SECONDS",
        ge=0,
        description="Live scan: max CLOB /book snapshot age (0 = disable)",
    )

    # ── Live engine (Phase 2) ──────────────────────────────────────────────
    paper_trading: bool = Field(
        default=True,
        validation_alias="PAPER_TRADING",
        description="When True: log orders but never hit CLOB POST /order",
    )
    # CLOB L2 credentials (from Polymarket UI → Settings → API Keys)
    clob_api_key: str | None = Field(default=None, validation_alias="POLYMARKET_API_KEY")
    clob_secret: str | None = Field(default=None, validation_alias="POLYMARKET_SECRET")
    clob_passphrase: str | None = Field(default=None, validation_alias="POLYMARKET_PASSPHRASE")

    # Crypto filter: allowlist of underlyings and active-hours window (UTC)
    engine_crypto_symbols: str = Field(
        default="",
        validation_alias="ENGINE_CRYPTO_SYMBOLS",
        description="Comma-separated crypto underlyings to trade (e.g. btc,eth). Empty = all.",
    )
    engine_crypto_active_hours: str = Field(
        default="",
        validation_alias="ENGINE_CRYPTO_ACTIVE_HOURS",
        description="UTC hour range for crypto trading, format 'START-END' (e.g. 8-20). Empty = 24h.",  # noqa: E501
    )

    # Stage 1: coarse monitoring interval for all crypto HOT markets
    engine_stage1_interval_s: float = Field(
        default=2.0,
        validation_alias="ENGINE_STAGE1_INTERVAL_S",
        description="Seconds between Stage-1 midpoint polls (all crypto HOT markets)",
    )
    # Stage 2: high-frequency interval for watchlisted markets
    engine_stage2_interval_s: float = Field(
        default=0.5,
        validation_alias="ENGINE_STAGE2_INTERVAL_S",
        description="Seconds between Stage-2 midpoint polls (watchlist)",
    )
    # Stop-loss thresholds (mid price below which open positions are closed)
    engine_stop_loss_crypto: float = Field(
        default=0.96,
        validation_alias="ENGINE_STOP_LOSS_CRYPTO",
        gt=0.0,
        lt=1.0,
    )
    engine_stop_loss_sports: float = Field(
        default=0.60,
        validation_alias="ENGINE_STOP_LOSS_SPORTS",
        gt=0.0,
        lt=1.0,
    )
    # Price threshold to move from Stage 1 → Stage 2
    engine_alert_threshold: float = Field(
        default=0.94,
        validation_alias="ENGINE_ALERT_THRESHOLD",
        description="YES or NO midpoint above this triggers Stage-2 watch",
    )
    # Max minutes left at which we allow entry, by market duration
    engine_max_entry_minutes_5m: float = Field(
        default=2.0,
        validation_alias="ENGINE_MAX_ENTRY_MINUTES_5M",
        description="5-min markets: only enter if minutes_left ≤ this",
    )
    engine_max_entry_minutes_15m: float = Field(
        default=5.0,
        validation_alias="ENGINE_MAX_ENTRY_MINUTES_15M",
        description="15-min markets: only enter if minutes_left ≤ this",
    )
    engine_max_entry_minutes_60m: float = Field(
        default=10.0,
        validation_alias="ENGINE_MAX_ENTRY_MINUTES_60M",
        description="1-hour markets: only enter if minutes_left ≤ this",
    )
    # Lookahead window for Stage-1 candidate fetch
    engine_monitor_horizon_minutes: float = Field(
        default=30.0,
        validation_alias="ENGINE_MONITOR_HORIZON_MINUTES",
        description="Crypto markets expiring within this window are Stage-1 candidates",
    )
    # Risk limits
    engine_max_open_positions: int = Field(
        default=10,
        validation_alias="ENGINE_MAX_OPEN_POSITIONS",
        description="Max concurrent open orders (base + chain combined)",
    )
    engine_max_notional_usd: float = Field(
        default=100.0,
        validation_alias="ENGINE_MAX_NOTIONAL_USD",
        description="Max USD deployed in base (chain_level=0) orders; chain orders are exempt",
    )
    engine_max_positions_per_underlying: int = Field(
        default=3,
        validation_alias="ENGINE_MAX_POSITIONS_PER_UNDERLYING",
        description="Max concurrent open orders for same asset (BTC, ETH, …)",
    )
    # Compound chain capital allocation
    engine_chain_enabled: bool = Field(
        default=True,
        validation_alias="ENGINE_CHAIN_ENABLED",
        description="Enable compound chain strategy: wins fund next order at compounded size",
    )
    engine_max_chains: int = Field(
        default=0,
        validation_alias="ENGINE_MAX_CHAINS",
        ge=0,
        description=(
            "Max distinct chains per session (0 = unlimited). "
            "Applies to new chain creation only; existing chains continue to compound."
        ),
    )

    # ── NearCT strategy ──────────────────────────────────────────────────────
    nearct_ask_min: float = Field(
        default=0.990,
        validation_alias="NEARCT_ASK_MIN",
        description="Minimum ask to enter a near-certainty position",
    )
    nearct_scan_horizon_min: float = Field(
        default=4.0,
        validation_alias="NEARCT_SCAN_HORIZON_MIN",
        description="Look-ahead window (minutes) for expiring markets",
    )
    nearct_min_depth_usd: float = Field(
        default=15.0,
        validation_alias="NEARCT_MIN_DEPTH_USD",
        description="Minimum book depth at ask (USD) to accept the market",
    )
    nearct_min_yield_pct: float = Field(
        default=0.006,
        validation_alias="NEARCT_MIN_YIELD_PCT",
        ge=0.0,
        le=0.05,
        description=(
            "Minimum net yield per trade (0.006 = 0.6%). Filters near-1.0 asks "
            "whose risk/reward is poor (e.g. ask 0.999 pays 0.09% but loses 100%)."
        ),
    )
    nearct_domains: str = Field(
        default="crypto",
        validation_alias="NEARCT_DOMAINS",
        description=(
            "Comma-separated domains to trade. Only crypto entries can be "
            "verified against the underlying price feed; sports/unknown at 0.99 "
            "near expiry are adversely selected (in-play flips)."
        ),
    )
    nearct_verify_base_major_pct: float = Field(
        default=0.0015,
        validation_alias="NEARCT_VERIFY_BASE_MAJOR_PCT",
        ge=0.0,
        le=0.05,
        description="BTC/ETH: required spot-to-strike distance per sqrt-minute (0.0015 = 0.15%)",
    )
    nearct_verify_base_alt_pct: float = Field(
        default=0.003,
        validation_alias="NEARCT_VERIFY_BASE_ALT_PCT",
        ge=0.0,
        le=0.05,
        description="Altcoins: required spot-to-strike distance per sqrt-minute (0.003 = 0.3%)",
    )
    # ── Maker quoting (v3) ──────────────────────────────────────────────────
    nearct_min_sigma: float = Field(
        default=8.0,
        validation_alias="NEARCT_MIN_SIGMA",
        ge=1.0,
        le=50.0,
        description="Certainty gate: spot-to-strike distance in realized-vol sigmas",
    )
    nearct_target_yield_pct: float = Field(
        default=0.018,
        validation_alias="NEARCT_TARGET_YIELD_PCT",
        gt=0.0,
        le=0.10,
        description="Maker bid priced at 1/(1+target): 0.018 -> bid 0.982, no maker fee",
    )
    nearct_quote_horizon_min: float = Field(
        default=90.0,
        validation_alias="NEARCT_QUOTE_HORIZON_MIN",
        ge=5.0,
        le=360.0,
        description="Quote markets expiring within this window (minutes)",
    )
    nearct_min_quote_minutes: float = Field(
        default=2.0,
        validation_alias="NEARCT_MIN_QUOTE_MINUTES",
        ge=0.5,
        le=30.0,
        description="Stop quoting a market when less than this is left",
    )
    # ── Sports maker ────────────────────────────────────────────────────────
    nearct_sports_enabled: bool = Field(
        default=True,
        validation_alias="NEARCT_SPORTS_ENABLED",
        description="Quote in-play sports outcomes verified by live trade consensus",
    )
    nearct_sports_target_yield_pct: float = Field(
        default=0.075,
        validation_alias="NEARCT_SPORTS_TARGET_YIELD_PCT",
        gt=0.0,
        le=0.25,
        description="Sports bid priced at 1/(1+target): 0.075 -> bid 0.930",
    )
    nearct_sports_min_volume: float = Field(
        default=20000.0,
        validation_alias="NEARCT_SPORTS_MIN_VOLUME",
        ge=0.0,
        description="Only quote sports markets with volumeNum above this (USD)",
    )
    nearct_sports_consensus: float = Field(
        default=0.96,
        validation_alias="NEARCT_SPORTS_CONSENSUS",
        gt=0.5,
        lt=1.0,
        description="Median of recent prints must clear this to quote the outcome",
    )
    nearct_sports_inplay_min: float = Field(
        default=45.0,
        validation_alias="NEARCT_SPORTS_INPLAY_MIN",
        ge=0.0,
        description="Quote only after this many minutes past kickoff (late-game states)",
    )
    nearct_start_capital: float = Field(
        default=10.0,
        validation_alias="NEARCT_START_CAPITAL",
        description="Starting capital; also the reset value after a loss",
    )
    nearct_max_order_usd: float = Field(
        default=500.0,
        validation_alias="NEARCT_MAX_ORDER_USD",
        description="Hard cap on single order size. Capital above this accumulates but is not bet.",
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
    def catalog_quote_horizon_hours(self) -> float:
        """Lookahead for WS + REST enrich (expiring slice)."""
        return min(168.0, self.catalog_expiring_hours)

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
