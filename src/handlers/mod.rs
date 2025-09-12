use std::time::Instant;

use clickhouse::Client;
use futures::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use near_lake_framework::near_indexer_primitives::StreamerMessage;

use crate::cache;

mod events;
mod receipts_and_outcomes;
mod transactions;

pub(crate) fn any_account_id_of_interest(account_ids: &[&str]) -> bool {
    account_ids
        .iter()
        .any(|id| crate::CONTRACT_ACCOUNT_IDS_OF_INTEREST.contains(id))
}

pub async fn handle_stream<T: Into<near_lake_framework::providers::NearLakeFrameworkConfig>>(
    config: T,
    client: Client,
    receipts_cache_arc: cache::ReceiptsCacheArc,
) -> anyhow::Result<()> {
    let (_, stream) = near_lake_framework::streamer(config.into());

    let mut handlers = ReceiverStream::new(stream)
        .map(|message| handle_streamer_message(message, &client, receipts_cache_arc.clone()))
        .buffer_unordered(1);

    while let Some(result) = handlers.next().await {
        result?; // Propagate error to interrupt the stream
    }
    Ok(())
}

#[tracing::instrument(
    name = "handle_streamer_message",
    skip(message, client, receipts_cache_arc),
    fields(
        block_height = message.block.header.height,
        block_hash = %message.block.header.hash
    )
)]
async fn handle_streamer_message(
    message: StreamerMessage,
    client: &Client,
    receipts_cache_arc: cache::ReceiptsCacheArc,
) -> anyhow::Result<()> {
    let start = Instant::now();
    crate::metrics::LATEST_BLOCK_HEIGHT.set(message.block.header.height as i64);
    tracing::info!("Block: {}", message.block.header.height);

    // We always process transactions first to populate the cache
    // with mappings from Receipt IDs to their parent Transaction hashes.
    transactions::handle_transactions(&message, client, receipts_cache_arc.clone()).await?;

    // Now process the remaining handlers in parallel with the optimized combined approach
    let receipts_and_outcomes_future = receipts_and_outcomes::handle_receipts_and_outcomes(
        &message,
        client,
        receipts_cache_arc.clone(),
    );

    let events_future = events::handle_events(&message, client, receipts_cache_arc.clone());

    // Run receipts+outcomes and events in parallel
    tokio::try_join!(receipts_and_outcomes_future, events_future)?;

    crate::metrics::BLOCK_PROCESSED_TOTAL.inc();
    let duration = start.elapsed();
    tracing::info!(
        duration_ms = duration.as_millis(),
        "handle_streamer_message completed"
    );
    Ok(())
}
