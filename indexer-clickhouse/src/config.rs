use clap::Parser;
use indexer_common::config::CommonConfig;

/// Application configuration loaded from CLI arguments and environment variables.
#[derive(Parser, Clone)]
#[clap(author, version, about)]
pub struct AppConfig {
    #[clap(flatten)]
    pub common: CommonConfig,

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

    /// Handle only events ignoring the transactions, receipts, outcomes
    #[clap(long, env = "EVENTS_ONLY_MODE")]
    pub events_only: bool,
}
