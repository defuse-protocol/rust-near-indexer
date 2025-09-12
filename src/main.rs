use clap::Parser;
#[macro_use]
extern crate lazy_static;

use crate::config::{AppConfig, init_tracing_with_otel};
use crate::database::{get_last_height, init_clickhouse_client};

mod cache;
mod config;
mod database;
mod handlers;
mod metrics;
mod types;

pub(crate) const CONTRACT_ACCOUNT_IDS_OF_INTEREST: &[&str] =
    &["intents.near", "defuse-alpha.near", "staging-intents.near"];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let config = AppConfig::parse();

    // Initialize tracing (with or without OpenTelemetry based on configuration)
    init_tracing_with_otel(&config).await?;

    // Expose version info metric once
    crate::metrics::VERSION_INFO
        .with_label_values(&[env!("CARGO_PKG_VERSION")])
        .set(1);

    let client = init_clickhouse_client(&config);

    let block_height: u64 = config.block_height;

    let last_height = get_last_height(&client).await?;
    let start_block = if config.force_from_block_height {
        tracing::warn!("Forcing reindex from block height: {}", block_height);
        block_height
    } else {
        std::cmp::max(block_height, last_height + 1)
    };

    tracing::info!("Starting indexer at block height: {}", start_block);

    let lake_config = near_lake_framework::LakeConfigBuilder::default()
        .mainnet()
        .start_block_height(start_block)
        .build()
        .expect("Error creating NEAR Lake framework config");

    // TODO: consider making the provider configurable (e.g., via CLI argument)
    // let lake_config = near_lake_framework::FastNearConfigBuilder::default()
    //     .mainnet()
    //     .start_block_height(start_block)
    //     .build()
    //     .expect("Error creating NEAR Lake framework config");

    let receipts_cache_arc: cache::ReceiptsCacheArc = cache::init_cache(&config).await?;

    // Initiate metrics http server
    if config.metrics_basic_auth_user.is_some() && config.metrics_basic_auth_password.is_some() {
        tracing::info!("Metrics server basic auth is enabled");
        tokio::spawn(metrics::init_server_with_basic_auth(
            config.metrics_server_port,
            (
                config
                    .metrics_basic_auth_user
                    .clone()
                    .expect("metrics_basic_auth_user is set"),
                config
                    .metrics_basic_auth_password
                    .clone()
                    .expect("metrics_basic_auth_password is set"),
            ),
        )?);
    } else {
        tracing::info!("Metrics server basic auth is disabled");
        tokio::spawn(metrics::init_server(config.metrics_server_port)?);
    };

    handlers::handle_stream(lake_config, client, receipts_cache_arc).await?;

    Ok(())
}
