mod config;
mod database;
mod handlers;

use std::time::Duration;

use clap::Parser;

use blocksapi::near_indexer_primitives::StreamerMessage;
use config::{AppConfig, DataSource};
use database::{get_last_height_events, get_last_height_transactions, init_clickhouse_client};
use indexer_common::cache;
use indexer_common::config::{BlockApiParams, init_tracing_with_otel};
use indexer_common::metrics;

/// Delay before rebuilding the block streamer after a transient producer error
/// (e.g. h2 "error reading a body from connection"). Small enough that the cache
/// stays warm; large enough that we don't hammer the upstream on a real outage.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(2);

/// Buffer between the Lake streamer and the 0.34 → 0.37 conversion task. Lake
/// already preloads blocks on its own side, so this only needs to absorb jitter.
const LAKE_CONVERTED_BUFFER: usize = 100;

/// Data-source settings validated once, before the reconnect loop.
enum StreamSource {
    Blocksapi(BlockApiParams),
    Lake,
}

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

    // Validate data-source-specific config once, up-front. The actual streamer is
    // rebuilt inside the reconnect loop below so we can resume from in-RAM state
    // after a transient producer error (e.g. h2 stream cut).
    let stream_source = match &config.data_source {
        DataSource::Blocksapi => StreamSource::Blocksapi(BlockApiParams {
            blocksapi_server_addr: config.blocksapi_server_addr.clone().ok_or_else(|| {
                anyhow::anyhow!("BLOCKSAPI_SERVER_ADDR is required when data_source is blocksapi")
            })?,
            blocksapi_token: config.blocksapi_token.clone().ok_or_else(|| {
                anyhow::anyhow!("BLOCKSAPI_TOKEN is required when data_source is blocksapi")
            })?,
        }),
        DataSource::Lake => {
            // Built here only to fail fast on bad config; rebuilt per reconnect below.
            build_lake_config(&config, start_block).await?;
            StreamSource::Lake
        }
    };

    let receipts_cache_arc: cache::ReceiptsCacheArc =
        cache::init_cache(&config.common.redis_url, config.common.redis_ttl_seconds).await?;
    let app_config = std::sync::Arc::new(config);

    // Initiate metrics http server
    metrics::spawn_metrics_server(&app_config.common)?;

    // In-RAM cursor of the highest block this process has successfully processed.
    // Reuses `metrics::LATEST_BLOCK_HEIGHT` (a `prometheus::IntGauge`, internally
    // atomic, set exactly once per successful block in `handle_streamer_message`).
    // MUST stay in-process — a reindexer and the live indexer share the same DB
    // at very different heights, so a DB `max(block_height)` would drag the
    // reindexer to the tip and silently abandon historical work in flight.
    let block_end = app_config.common.block_end;

    loop {
        let resume_from = match metrics::LATEST_BLOCK_HEIGHT.get() {
            0 => start_block,
            h => (h as u64) + 1,
        };

        if let Some(end) = block_end
            && resume_from > end
        {
            tracing::info!(
                target: indexer_common::config::INDEXER,
                "block_end={} already reached at {}, exiting.",
                end,
                resume_from - 1
            );
            break;
        }

        tracing::info!(
            target: indexer_common::config::INDEXER,
            "Building block stream from block {}",
            resume_from
        );
        let (producer_handle, stream) = match &stream_source {
            StreamSource::Blocksapi(blocksapi_params) => blocksapi::streamer(
                indexer_common::config::build_blocksapi_config(blocksapi_params, resume_from),
            ),
            StreamSource::Lake => lake_streamer(build_lake_config(&app_config, resume_from).await?),
        };

        tokio::select! {
            result = handlers::handle_stream(
                stream,
                client.clone(),
                receipts_cache_arc.clone(),
                app_config.clone(),
            ) => {
                result?;
                // Consumer returned Ok — either block_end was reached (handled at
                // top of next iteration) or the producer dropped the channel and
                // the stream drained cleanly. In the latter case, loop to reconnect.
                let last = metrics::LATEST_BLOCK_HEIGHT.get();
                if let Some(end) = block_end
                    && (last as u64) >= end
                {
                    break;
                }
                tracing::warn!(
                    target: indexer_common::config::INDEXER,
                    "Stream ended without reaching block_end (last_processed={}); reconnecting after {:?}",
                    last,
                    RECONNECT_BACKOFF
                );
            }
            result = producer_handle => {
                match result {
                    Ok(Ok(())) => tracing::warn!(
                        target: indexer_common::config::INDEXER,
                        "Producer task finished unexpectedly; reconnecting"
                    ),
                    // Deterministic, so reconnecting would retry the same block forever.
                    // Both the Lake 0.34 → 0.37 conversion and a malformed Lake block
                    // land here.
                    Ok(Err(e)) if e.is::<serde_json::Error>() => {
                        return Err(e.context("Failed to decode block from the stream"));
                    }
                    Ok(Err(e)) => tracing::warn!(
                        target: indexer_common::config::INDEXER,
                        error = %e,
                        "Producer stream error; reconnecting"
                    ),
                    Err(e) => tracing::warn!(
                        target: indexer_common::config::INDEXER,
                        error = %e,
                        "Producer task panicked or was cancelled; reconnecting"
                    ),
                }
            }
        }

        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }

    Ok(())
}

/// Lake streamer adapted to the `StreamerMessage` type the handlers use.
///
/// `near-lake-framework` 0.7 is pinned to `near-indexer-primitives` 0.34, while
/// `blocksapi` (and so our handlers) are on 0.37. The two `StreamerMessage` types
/// are distinct to the compiler but share the same serde shape (Lake reads blocks
/// as JSON in the first place), so each block goes through a JSON round-trip.
/// Drop this once near-lake-framework ships on the same primitives as blocksapi.
fn lake_streamer(
    lake_config: near_lake_framework::LakeConfig,
) -> (
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tokio::sync::mpsc::Receiver<StreamerMessage>,
) {
    let (lake_handle, mut lake_stream) = near_lake_framework::streamer(lake_config);
    let (sender, receiver) = tokio::sync::mpsc::channel(LAKE_CONVERTED_BUFFER);
    let converter_handle = tokio::spawn(async move {
        while let Some(lake_message) = lake_stream.recv().await {
            let message: StreamerMessage =
                serde_json::from_value(serde_json::to_value(&lake_message)?)?;
            if sender.send(message).await.is_err() {
                // Consumer is gone (reconnect or shutdown). Returning drops
                // `lake_stream`, which stops the Lake task as well.
                return Ok(());
            }
        }
        // Lake closed its channel: surface its own result (error or clean end).
        lake_handle.await?
    });
    (converter_handle, receiver)
}

async fn build_lake_config(
    config: &AppConfig,
    start_block: u64,
) -> anyhow::Result<near_lake_framework::LakeConfig> {
    let mut builder = near_lake_framework::LakeConfigBuilder::default()
        .s3_bucket_name(config.lake_s3_bucket.as_ref().ok_or_else(|| {
            anyhow::anyhow!("LAKE_S3_BUCKET is required when data_source is lake")
        })?)
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

    builder
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build LakeConfig: {e}"))
}
