use std::time::Instant;

use near_lake_framework::near_indexer_primitives::{self, near_primitives};

use crate::{cache, types};

const RECEIPTS_CLICKHOUSE_TABLE: &str = "receipts";

/// Extract receipts from the StreamerMessage,
/// store receipts in Clickhouse, and update the receipts cache with
/// mappings from receipt IDs to their parent transaction hashes.
/// This function processes only receipts related to accounts of interest.
/// It uses the provided Clickhouse client for database operations and
/// a shared receipts cache to maintain the relationship between receipts and transactions.
pub async fn handle_receipts(
    message: &near_indexer_primitives::StreamerMessage,
    client: &clickhouse::Client,
    receipts_cache_arc: cache::ReceiptsCacheArc,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let receipts = extract_receipts(message, receipts_cache_arc.clone()).await?;

    if let Err(err) =
        crate::database::insert_rows(client, RECEIPTS_CLICKHOUSE_TABLE, &receipts).await
    {
        tracing::error!("Error inserting rows into Clickhouse: {}", err);
        anyhow::bail!("Failed to insert rows into Clickhouse: {}", err)
    }
    tracing::debug!("handle_receipts {:?}", start.elapsed());
    Ok(())
}

async fn extract_receipts(
    message: &near_indexer_primitives::StreamerMessage,
    receipts_cache_arc: cache::ReceiptsCacheArc,
) -> anyhow::Result<Vec<types::ReceiptRow>> {
    let start = Instant::now();
    let receipts_cache = receipts_cache_arc.clone();
    let block_height = message.block.header.height;
    let block_timestamp = message.block.header.timestamp;
    let block_hash = message.block.header.hash.to_string();
    let receipts = message
        .shards
        .iter()
        .filter_map(|shard| shard.chunk.as_ref())
        .flat_map(|chunk| chunk.receipts.iter().map(Clone::clone))
        .filter_map(|receipt| {
            if let Some(parent_tx_hash) = receipts_cache.get(&types::ReceiptOrDataId::ReceiptId(receipt.receipt_id)) {
                Some(types::ReceiptRow{
                    block_height,
                    block_timestamp,
                    block_hash: block_hash.clone(),
                    parent_transaction_hash: parent_tx_hash,
                    receipt_id: receipt.receipt_id.clone().to_string(),
                    receiver_id: receipt.receiver_id.clone().to_string(),
                    predecessor_id: receipt.predecessor_id.clone().to_string(),
                    actions: match receipt.receipt {
                        near_primitives::views::ReceiptEnumView::Action { ref actions, .. } => {
                            serde_json::to_string(
                                &actions.iter().map(types::Action::from).collect::<Vec<types::Action>>()
                            ).expect("Failed to serialize actions for receipt")
                        },
                        near_primitives::views::ReceiptEnumView::Data { ref data, .. } => {
                            serde_json::to_string(data).unwrap()
                        },
                        near_primitives::views::ReceiptEnumView::GlobalContractDistribution { .. } => {
                            "".to_string()
                        },
                    },
                })
            } else if super::any_account_id_of_interest(&[
                receipt.receiver_id.as_str(),
                receipt.predecessor_id.as_str(),
            ]) {
                tracing::warn!("We don't watch for the this receipt but it is related to the account ids of interest, {}", receipt.receipt_id);
                tracing::debug!("{:#?}", receipt.receipt);
                crate::metrics::POTENTIAL_ASSET_MISS_TOTAL.with_label_values(&["receipts"]).inc();

                // We check the potential cache expecting to find a mapping there
                if let Some(parent_tx_hash) = receipts_cache.potential_get(&types::ReceiptOrDataId::ReceiptId(receipt.receipt_id)) {
                    // Protomote this cache entry to the main cache to catch the outcome
                    receipts_cache.set(types::ReceiptOrDataId::ReceiptId(receipt.receipt_id), parent_tx_hash.clone());

                    tracing::info!("Found a potential mapping for receipt {} to transaction {}", receipt.receipt_id, parent_tx_hash);
                    Some(types::ReceiptRow{
                        block_height,
                        block_timestamp,
                        block_hash: block_hash.clone(),
                        parent_transaction_hash: parent_tx_hash,
                        receipt_id: receipt.receipt_id.clone().to_string(),
                        receiver_id: receipt.receiver_id.clone().to_string(),
                        predecessor_id: receipt.predecessor_id.clone().to_string(),
                        actions: match receipt.receipt {
                            near_primitives::views::ReceiptEnumView::Action { ref actions, .. } => {
                                serde_json::to_string(
                                    &actions.iter().map(types::Action::from).collect::<Vec<types::Action>>()
                                ).expect("Failed to serialize actions for receipt")
                            },
                            near_primitives::views::ReceiptEnumView::Data { ref data, .. } => {
                                serde_json::to_string(data).unwrap()
                            },
                            near_primitives::views::ReceiptEnumView::GlobalContractDistribution { .. } => {
                                "".to_string()
                            },
                        },
                    })
                } else {
                    tracing::warn!("No potential mapping found for receipt {}", receipt.receipt_id);
                    // TODO: consider inc a metric for potential cache miss
                    None
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    drop(receipts_cache);
    crate::metrics::ASSETS_IN_BLOCK_TOTAL
        .with_label_values(&["receipts"])
        .set(
            message
                .shards
                .iter()
                .filter_map(|shard| shard.chunk.as_ref())
                .map(|chunk| chunk.receipts.len() as i64)
                .sum(),
        );
    crate::metrics::ASSETS_IN_BLOCK_CAPTURED_TOTAL
        .with_label_values(&["receipts"])
        .set(receipts.len() as i64);
    tracing::debug!("extract_receipts {:?}", start.elapsed());
    Ok(receipts)
}
