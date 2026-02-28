mod config;
mod database;
mod handlers;

use clap::Parser;

use config::AppConfig;
use database::{get_last_height_events, get_last_height_transactions, init_clickhouse_client};
use indexer_common::cache;
use indexer_common::config::init_tracing_with_otel;
use indexer_common::metrics;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let config = AppConfig::parse();

    // Initialize tracing (with or without OpenTelemetry based on configuration)
    let otel = config.common.otel_config();
    init_tracing_with_otel(otel.as_ref()).await?;

    // Expose version info metric once
    metrics::VERSION_INFO
        .with_label_values(&[env!("CARGO_PKG_VERSION")])
        .set(1);

    let client = init_clickhouse_client(&config);

    let block_height: u64 = config.common.block_height;

    let last_height = if config.events_only {
        tracing::warn!(
            "EVENTS ONLY mode is active, all transactions, receipts and execution outcomes will be ignored by this indexer!"
        );
        get_last_height_events(&client).await?
    } else {
        get_last_height_transactions(&client).await?
    };

    let start_block = if config.common.force_from_block_height {
        tracing::warn!(
            target: indexer_common::config::INDEXER,
            "Forcing reindex from block height: {}",
            block_height
        );
        block_height
    } else {
        std::cmp::max(block_height, last_height + 1)
    };

    tracing::info!(
        target: indexer_common::config::INDEXER,
        "Starting indexer at block height: {}",
        start_block
    );

    let blocksapi_config =
        indexer_common::config::build_blocksapi_config(&config.common, start_block);

    let receipts_cache_arc: cache::ReceiptsCacheArc =
        cache::init_cache(&config.common.redis_url, config.common.redis_ttl_seconds).await?;
    let app_config = std::sync::Arc::new(config);

    // Initiate metrics http server
    metrics::spawn_metrics_server(&app_config.common)?;

    handlers::handle_stream(blocksapi_config, client, receipts_cache_arc, app_config).await?;

    Ok(())
}
