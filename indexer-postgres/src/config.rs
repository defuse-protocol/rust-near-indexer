use clap::Parser;

/// Application configuration for the Postgres indexer.
#[derive(Parser, Clone)]
#[clap(author, version, about = "NEAR Defuse Indexer — PostgreSQL backend")]
pub struct AppConfig {
    /// Fallback block height to start if no previous height is found in the database
    #[clap(long, env = "BLOCK_HEIGHT", default_value = "0")]
    pub block_height: u64,

    /// Stop the indexer after processing this block height (env: BLOCK_END)
    #[clap(long, env = "BLOCK_END")]
    pub block_end: Option<u64>,

    /// Forces the indexer to start from the specified block height provided via --block-height,
    /// even if a higher block height is found in the database.
    #[clap(long, requires = "block_height", hide = true)]
    pub force_from_block_height: bool,

    /// PostgreSQL connection URL (env: DATABASE_URL)
    #[clap(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// Redis server URL for receipt-to-tx cache (env: REDIS_URL)
    #[clap(long, env = "REDIS_URL")]
    pub redis_url: String,

    /// Redis cache TTL in seconds (env: REDIS_TTL_SECONDS, default: 900)
    #[clap(long, env = "REDIS_TTL_SECONDS", default_value = "900")]
    pub redis_ttl_seconds: u64,

    /// Metrics server port (env: METRICS_SERVER_PORT, default: 8081)
    #[clap(long, env = "METRICS_SERVER_PORT", default_value = "8081")]
    pub metrics_server_port: u16,

    /// Metrics server basic auth username (env: METRICS_BASIC_AUTH_USER)
    #[clap(
        long,
        env = "METRICS_BASIC_AUTH_USER",
        requires = "metrics_basic_auth_password"
    )]
    pub metrics_basic_auth_user: Option<String>,

    /// Metrics server basic auth password (env: METRICS_BASIC_AUTH_PASSWORD)
    #[clap(
        long,
        env = "METRICS_BASIC_AUTH_PASSWORD",
        requires = "metrics_basic_auth_user"
    )]
    pub metrics_basic_auth_password: Option<String>,

    /// OpenTelemetry OTLP endpoint for trace export (env: OTEL_EXPORTER_OTLP_ENDPOINT)
    #[clap(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub otel_endpoint: Option<String>,

    /// Service name for tracing (env: OTEL_SERVICE_NAME, default: indexer-postgres)
    #[clap(long, env = "OTEL_SERVICE_NAME", default_value = "indexer-postgres")]
    pub otel_service_name: String,

    /// Service version for tracing (env: OTEL_SERVICE_VERSION, default: 0.1.0)
    #[clap(long, env = "OTEL_SERVICE_VERSION", default_value = "0.1.0")]
    pub otel_service_version: String,

    /// Concurrency level for per-outcome processing (env: OUTCOME_CONCURRENCY, default: 32)
    #[clap(long, env = "OUTCOME_CONCURRENCY", default_value = "32")]
    pub outcome_concurrency: usize,

    /// Blocks API server address (env: BLOCKSAPI_SERVER_ADDR)
    #[clap(long, env = "BLOCKSAPI_SERVER_ADDR")]
    pub blocksapi_server_addr: String,

    /// Blocks API token (env: BLOCKSAPI_TOKEN)
    #[clap(long, env = "BLOCKSAPI_TOKEN")]
    pub blocksapi_token: String,
}
