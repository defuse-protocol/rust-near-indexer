mod config;
mod database;
mod handlers;

use clap::Parser;

use config::AppConfig;
use indexer_common::cache;
use indexer_common::config::{OtelConfig, init_tracing_with_otel};
use indexer_common::metrics;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let config = AppConfig::parse();

    // Initialize tracing (with or without OpenTelemetry based on configuration)
    let otel = config.otel_endpoint.as_ref().map(|endpoint| OtelConfig {
        endpoint: endpoint.clone(),
        service_name: config.otel_service_name.clone(),
        service_version: config.otel_service_version.clone(),
    });
    init_tracing_with_otel(otel.as_ref()).await?;

    // Expose version info metric once
    metrics::VERSION_INFO
        .with_label_values(&[env!("CARGO_PKG_VERSION")])
        .set(1);

    // Init Postgres pool + run migrations
    let pool = database::init_pg_pool(&config.database_url).await?;

    // Determine start block
    let block_height: u64 = config.block_height;
    let last_height = database::get_last_block_height(&pool).await?;

    let start_block = if config.force_from_block_height {
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
        cache::init_cache(&config.redis_url, config.redis_ttl_seconds).await?;

    // Initiate metrics http server
    if config.metrics_basic_auth_user.is_some() && config.metrics_basic_auth_password.is_some() {
        tracing::info!(
            target: indexer_common::config::INDEXER,
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
            target: indexer_common::config::INDEXER,
            "Metrics server basic auth is disabled"
        );
        tokio::spawn(metrics::init_server(config.metrics_server_port)?);
    };

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

    let app_config = std::sync::Arc::new(config);

    handlers::handle_stream(blocksapi_config, pool, receipts_cache_arc, app_config).await?;

    Ok(())
}
