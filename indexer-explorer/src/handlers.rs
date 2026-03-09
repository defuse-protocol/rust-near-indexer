use std::time::Instant;

use futures::StreamExt;
use sqlx::PgPool;
use tokio_stream::wrappers::ReceiverStream;

use blocksapi::near_indexer_primitives::StreamerMessage;

use indexer_common::cache;
use indexer_common::extractors;

use crate::config::AppConfig;

const ACCOUNTS_OF_INTEREST: &[&str] =
    &["intents.near", "defuse-alpha.near", "staging-intents.near"];
const PRODUCTION_CONTRACT_IDS: &[&str] = &["defuse-alpha.near", "intents.near"];

pub async fn handle_stream(
    config: blocksapi::BlocksApiConfig,
    pool: PgPool,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    app_config: std::sync::Arc<AppConfig>,
) -> anyhow::Result<()> {
    let (_, stream) = blocksapi::streamer(config);

    let block_end = app_config.common.block_end;
    if let Some(end) = block_end {
        tracing::info!(
            target: indexer_common::config::INDEXER,
            "Indexer will stop after block height: {}",
            end
        );
    }

    let accounts: Vec<String> = ACCOUNTS_OF_INTEREST.iter().map(|s| s.to_string()).collect();

    let mut handlers = ReceiverStream::new(stream)
        .map(|message| {
            handle_streamer_message(
                message,
                &pool,
                receipts_cache_arc.clone(),
                app_config.clone(),
                &accounts,
            )
        })
        .buffer_unordered(1);

    while let Some(result) = handlers.next().await {
        let block_height = result?;
        if let Some(end) = block_end
            && block_height >= end
        {
            tracing::info!(
                target: indexer_common::config::INDEXER,
                "Reached block_end={}, stopping.",
                end
            );
            break;
        }
    }
    Ok(())
}

#[tracing::instrument(
    name = "handle_streamer_message",
    skip(message, pool, receipts_cache_arc, app_config, accounts),
    fields(
        block_height = message.block.header.height,
        block_hash = %message.block.header.hash
    )
)]
async fn handle_streamer_message(
    message: StreamerMessage,
    pool: &PgPool,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    app_config: std::sync::Arc<AppConfig>,
    accounts: &[String],
) -> anyhow::Result<u64> {
    let start = Instant::now();
    let block_height = message.block.header.height;
    tracing::info!(
        target: indexer_common::config::INDEXER,
        "Block: {}",
        block_height
    );

    // Process transactions to populate the receipt-to-tx cache
    extractors::transactions::extract_transactions(&message, receipts_cache_arc.clone(), accounts)
        .await?;

    // Extract raw events
    let events = extractors::events::extract_events(
        &message,
        receipts_cache_arc.clone(),
        app_config.common.outcome_concurrency,
        accounts,
    )
    .await?;

    // Parse events into silver DIP-4 transfer rows
    let transfer_rows = extractors::silver_transfers::extract_silver_dip4_transfers(
        &events,
        PRODUCTION_CONTRACT_IDS,
    );

    // Insert into Postgres
    crate::database::insert_transfer_rows(pool, &transfer_rows).await?;

    indexer_common::metrics::LATEST_BLOCK_HEIGHT.set(block_height as i64);
    indexer_common::metrics::BLOCK_PROCESSED_TOTAL.inc();
    let duration = start.elapsed();
    tracing::info!(
        target: indexer_common::config::INDEXER,
        duration_ms = duration.as_millis(),
        transfers = transfer_rows.len(),
        "handle_streamer_message completed"
    );
    Ok(block_height)
}
