use clap::Parser;
use indexer_common::config::CommonConfig;

#[derive(Clone, clap::ValueEnum)]
pub enum DataSource {
    Blocksapi,
    Lake,
}

/// Application configuration loaded from CLI arguments and environment variables.
#[derive(Parser, Clone)]
#[clap(author, version, about)]
pub struct AppConfig {
    #[clap(flatten)]
    pub common: CommonConfig,

    /// Data source: "blocksapi" or "lake" (env: DATA_SOURCE)
    #[clap(long, env = "DATA_SOURCE", value_enum, default_value = "blocksapi")]
    pub data_source: DataSource,

    // --- BlocksAPI fields (required when data_source=blocksapi) ---
    /// BlocksAPI server address (env: BLOCKSAPI_SERVER_ADDR)
    #[clap(
        long,
        env = "BLOCKSAPI_SERVER_ADDR",
        required_if_eq("data_source", "blocksapi")
    )]
    pub blocksapi_server_addr: Option<String>,

    /// BlocksAPI token (env: BLOCKSAPI_TOKEN)
    #[clap(
        long,
        env = "BLOCKSAPI_TOKEN",
        required_if_eq("data_source", "blocksapi")
    )]
    pub blocksapi_token: Option<String>,

    // --- Lake S3/GCS fields (required when data_source=lake) ---
    /// S3 bucket name containing NEAR Lake data (env: LAKE_S3_BUCKET)
    #[clap(long, env = "LAKE_S3_BUCKET", required_if_eq("data_source", "lake"))]
    pub lake_s3_bucket: Option<String>,

    /// S3 region name (env: LAKE_S3_REGION, default: us-east-1)
    #[clap(long, env = "LAKE_S3_REGION", default_value = "us-east-1")]
    pub lake_s3_region: String,

    /// Custom S3 endpoint URL, e.g. for GCS S3-compatible access (env: LAKE_S3_ENDPOINT)
    #[clap(long, env = "LAKE_S3_ENDPOINT")]
    pub lake_s3_endpoint: Option<String>,

    // --- Accounts of interest ---
    /// Comma-separated list of account IDs to track (env: ACCOUNTS_OF_INTEREST)
    #[clap(long, env = "ACCOUNTS_OF_INTEREST", value_delimiter = ',',
           default_values_t = ["intents.near".to_string(), "defuse-alpha.near".to_string(), "staging-intents.near".to_string()])]
    pub accounts_of_interest: Vec<String>,

    // --- ClickHouse fields ---
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
