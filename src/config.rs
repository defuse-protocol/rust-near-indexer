use clap::Parser;

/// Application configuration loaded from CLI arguments and environment variables.
#[derive(Parser, Clone)]
#[clap(author, version, about)]
pub struct AppConfig {
    // Fallback block height to start if no previous height is found in the database
    #[clap(long, env = "BLOCK_HEIGHT", default_value = "0")]
    pub block_height: u64,

    /// Clickhouse server URL (env: CLICKHOUSE_URL)
    #[clap(long, env = "CLICKHOUSE_URL")]
    pub clickhouse_url: String,

    /// Clickhouse username (env: CLICKHOUSE_USER)
    #[clap(long, env = "CLICKHOUSE_USER")]
    pub clickhouse_user: String,

    /// Clickhouse password (env: CLICKHOUSE_PASSWORD)
    #[clap(long, env = "CLICKHOUSE_PASSWORD")]
    pub clickhouse_password: String,

    /// Clickhouse database name (env: CLICKHOUSE_DATABASE)
    #[clap(long, env = "CLICKHOUSE_DATABASE")]
    pub clickhouse_database: String,

    /// Redis server URL (env: REDIS_URL)
    #[clap(long, env = "REDIS_URL")]
    pub redis_url: Option<String>,

    /// Redis cache TTL in seconds (env: REDIS_TTL_SECONDS, default: 900)
    #[clap(long, env = "REDIS_TTL_SECONDS", default_value = "900")]
    pub redis_ttl_seconds: u64,

    /// Metrics server port (env: METRICS_SERVER_PORT, default: 8080)
    #[clap(long, env = "METRICS_SERVER_PORT", default_value = "8080")]
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

    /// Forces the indexer to start from the specified block height provided via --block-height,
    /// even if a higher block height is found in the database. This is useful when reindexing
    /// from a specific point is needed.
    #[clap(long, requires = "block_height", hide = true)]
    pub force_from_block_height: bool,
}

pub fn init_tracing() {
    let rust_log = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "near_defuse_indexer=info,near_lake_framework=info".to_string());
    let env_filter = tracing_subscriber::EnvFilter::new(rust_log);
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
}
