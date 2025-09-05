use clickhouse::Client;
use futures::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

mod events;
mod execution_outcomes;
mod receipts;
mod transactions;

use near_lake_framework::near_indexer_primitives::StreamerMessage;

use crate::types::{self};

pub(crate) fn any_account_id_of_interest(account_ids: &[&str]) -> bool {
    account_ids
        .iter()
        .any(|id| crate::CONTRACT_ACCOUNT_IDS_OF_INTEREST.contains(id))
}

pub async fn handle_stream<T: Into<near_lake_framework::providers::NearLakeFrameworkConfig>>(
    config: T,
    client: Client,
    receipts_cache_arc: types::ReceiptsCacheArc,
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

async fn handle_streamer_message(
    message: StreamerMessage,
    client: &Client,
    receipts_cache_arc: types::ReceiptsCacheArc,
) -> anyhow::Result<()> {
    println!("Block: {}", message.block.header.height);
    // TODO: remove it after tests
    if message.block.header.height > 162389776 {
        std::process::exit(0);
    }

    let transactions_future =
        transactions::handle_transactions(&message, client, receipts_cache_arc.clone());
    let execution_outcomes_future =
        execution_outcomes::handle_execution_outcomes(&message, client, receipts_cache_arc.clone());
    let receipts_future = receipts::handle_receipts(&message, client, receipts_cache_arc.clone());
    let events_future = events::handle_events(&message, client, receipts_cache_arc.clone());

    futures::try_join!(
        transactions_future,
        execution_outcomes_future,
        receipts_future,
        events_future
    )?;
    Ok(())
}
