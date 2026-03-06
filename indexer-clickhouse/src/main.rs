mod config;
mod database;
mod handlers;

use clap::Parser;

use config::{AppConfig, DataSource};
use database::{get_last_height_events, get_last_height_transactions, init_clickhouse_client};
use indexer_common::cache;
use indexer_common::config::{BlocksApiFields, init_tracing_with_otel};
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

    // Build data-source-specific stream receiver
    let stream = match &config.data_source {
        DataSource::Blocksapi => {
            let ba_fields = BlocksApiFields {
                blocksapi_server_addr: config.blocksapi_server_addr.clone().unwrap(),
                blocksapi_token: config.blocksapi_token.clone().unwrap(),
            };
            let blocksapi_config =
                indexer_common::config::build_blocksapi_config(&ba_fields, start_block);
            let (_, stream) = blocksapi::streamer(blocksapi_config);
            stream
        }
        DataSource::Lake => {
            let lake_config = build_lake_config(&config, start_block).await?;
            let (_, stream) = near_lake_framework::streamer(lake_config);
            stream
        }
    };

    let receipts_cache_arc: cache::ReceiptsCacheArc =
        cache::init_cache(&config.common.redis_url, config.common.redis_ttl_seconds).await?;
    let app_config = std::sync::Arc::new(config);

    // Initiate metrics http server
    metrics::spawn_metrics_server(&app_config.common)?;

    handlers::handle_stream(stream, client, receipts_cache_arc, app_config).await?;

    Ok(())
}

async fn build_lake_config(
    config: &AppConfig,
    start_block: u64,
) -> anyhow::Result<near_lake_framework::LakeConfig> {
    let mut builder = near_lake_framework::LakeConfigBuilder::default()
        .s3_bucket_name(config.lake_s3_bucket.as_ref().unwrap())
        .s3_region_name(&config.lake_s3_region)
        .start_block_height(start_block);

    if let Some(endpoint) = &config.lake_s3_endpoint {
        // Build a custom S3 config with the custom endpoint for GCS S3-compatible access
        let aws_config = aws_config::from_env()
            .region(aws_config::Region::new(config.lake_s3_region.clone()))
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build();
        builder = builder.s3_config(s3_config);
    }

    Ok(builder.build().expect("Failed to build LakeConfig"))
}
