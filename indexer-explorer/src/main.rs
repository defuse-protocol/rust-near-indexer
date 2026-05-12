mod config;
mod database;
mod handlers;

use std::time::Duration;

use clap::Parser;

use config::AppConfig;
use indexer_common::cache;
use indexer_common::config::init_tracing_with_otel;
use indexer_common::metrics;

/// Delay before rebuilding the BlocksAPI streamer after a transient producer error
/// (e.g. h2 "error reading a body from connection"). Matches the clickhouse indexer.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(2);

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

    let app_config = std::sync::Arc::new(config);

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
            "Building BlocksAPI stream from block {}",
            resume_from
        );
        let blocksapi_config =
            indexer_common::config::build_blocksapi_config(&app_config.blocksapi, resume_from);
        let (producer_handle, stream) = blocksapi::streamer(blocksapi_config);

        tokio::select! {
            result = handlers::handle_stream(
                stream,
                pool.clone(),
                receipts_cache_arc.clone(),
                app_config.clone(),
            ) => {
                result?;
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
                        "BlocksAPI producer task finished unexpectedly; reconnecting"
                    ),
                    Ok(Err(e)) => tracing::warn!(
                        target: indexer_common::config::INDEXER,
                        error = %e,
                        "BlocksAPI producer stream error; reconnecting"
                    ),
                    Err(e) => tracing::warn!(
                        target: indexer_common::config::INDEXER,
                        error = %e,
                        "BlocksAPI producer task panicked or was cancelled; reconnecting"
                    ),
                }
            }
        }

        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }

    Ok(())
}
