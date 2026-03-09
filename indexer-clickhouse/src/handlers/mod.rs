use std::time::Instant;

use clickhouse::Client;
use futures::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use blocksapi::near_indexer_primitives::StreamerMessage;

use crate::config::AppConfig;
use indexer_common::cache;

mod events;
mod receipts_and_outcomes;
mod transactions;

pub async fn handle_stream(
    stream: tokio::sync::mpsc::Receiver<StreamerMessage>,
    client: Client,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    app_config: std::sync::Arc<AppConfig>,
) -> anyhow::Result<()> {
    let block_end = app_config.common.block_end;
    if let Some(end) = block_end {
        tracing::info!(
            target: indexer_common::config::INDEXER,
            "Indexer will stop after block height: {}",
            end
        );
    }

    let mut handlers = ReceiverStream::new(stream)
        .map(|message| {
            handle_streamer_message(
                message,
                &client,
                receipts_cache_arc.clone(),
                app_config.clone(),
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
    skip(message, client, receipts_cache_arc, app_config),
    fields(
        block_height = message.block.header.height,
        block_hash = %message.block.header.hash
    )
)]
async fn handle_streamer_message(
    message: StreamerMessage,
    client: &Client,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    app_config: std::sync::Arc<AppConfig>,
) -> anyhow::Result<u64> {
    let start = Instant::now();
    let block_height = message.block.header.height;
    let accounts = &app_config.accounts_of_interest;
    tracing::info!(
        target: indexer_common::config::INDEXER,
        "Block: {}",
        message.block.header.height
    );

    if !app_config.events_only {
        // We always process transactions first to populate the cache
        // with mappings from Receipt IDs to their parent Transaction hashes.
        transactions::handle_transactions(&message, client, receipts_cache_arc.clone(), accounts)
            .await?;
    }

    let events_future = events::handle_events(
        &message,
        client,
        receipts_cache_arc.clone(),
        app_config.common.outcome_concurrency,
        accounts,
    );

    if !app_config.events_only {
        let receipts_and_outcomes_future = receipts_and_outcomes::handle_receipts_and_outcomes(
            &message,
            client,
            receipts_cache_arc.clone(),
            app_config.common.outcome_concurrency,
            accounts,
        );

        // Run receipts+outcomes and events in parallel
        tokio::try_join!(receipts_and_outcomes_future, events_future)?;
    } else {
        events_future.await?;
    }

    indexer_common::metrics::LATEST_BLOCK_HEIGHT.set(block_height as i64);
    indexer_common::metrics::BLOCK_PROCESSED_TOTAL.inc();
    let duration = start.elapsed();
    tracing::info!(
        target: indexer_common::config::INDEXER,
        duration_ms = duration.as_millis(),
        "handle_streamer_message completed"
    );
    Ok(block_height)
}
