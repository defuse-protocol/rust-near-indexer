mod config;
mod database;
mod handlers;

use clap::Parser;

use config::AppConfig;
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

    // Init Postgres pool + run migrations
    let pool = database::init_pg_pool(&config.database_url).await?;

    // Determine start block
    let block_height: u64 = config.common.block_height;
    let last_height = database::get_last_block_height(&pool).await?;

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

    // Init Redis receipt cache
    let receipts_cache_arc: cache::ReceiptsCacheArc =
        cache::init_cache(&config.common.redis_url, config.common.redis_ttl_seconds).await?;

    // Initiate metrics http server
    metrics::spawn_metrics_server(&config.common)?;

    let blocksapi_config =
        indexer_common::config::build_blocksapi_config(&config.common, start_block);

    let app_config = std::sync::Arc::new(config);

    handlers::handle_stream(blocksapi_config, pool, receipts_cache_arc, app_config).await?;

    Ok(())
}
