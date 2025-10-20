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
        tracing::warn!(
            target: crate::config::INDEXER,
            "Forcing reindex from block height: {}",
            block_height
        );
        block_height
    } else {
        std::cmp::max(block_height, last_height + 1)
    };

    tracing::info!(
        target: crate::config::INDEXER,
        "Starting indexer at block height: {}",
        start_block
    );

    let blocksapi_config = blocksapi::BlocksApiConfigBuilder::default()
        .server_addr(config.blocksapi_server_addr.clone())
        .start_on(Some(start_block))
        .blocksapi_token(Some(config.blocksapi_token.clone()))
        .batch_size(30)
        .concurrency(1000)
        .buffer_size(2 * 1024 * 1024 * 1024)
        .concurrency_limit(2048)
        .build()
        .expect("Error creating Blocks API config");

    let receipts_cache_arc: cache::ReceiptsCacheArc = cache::init_cache(&config).await?;
    let app_config = std::sync::Arc::new(config.clone());

    // Initiate metrics http server
    if config.metrics_basic_auth_user.is_some() && config.metrics_basic_auth_password.is_some() {
        tracing::info!(
            target: crate::config::INDEXER,
            "Metrics server basic auth is enabled"
        );
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
        tracing::info!(
            target: crate::config::INDEXER,
            "Metrics server basic auth is disabled"
        );
        tokio::spawn(metrics::init_server(config.metrics_server_port)?);
    };

    handlers::handle_stream(blocksapi_config, client, receipts_cache_arc, app_config).await?;

    Ok(())
}
