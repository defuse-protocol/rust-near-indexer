use std::str::FromStr;
use std::time::Instant;

use near_lake_framework::near_indexer_primitives::{self, near_primitives};

use crate::{cache, handlers::events::parse_status, types};

pub(crate) const EXECUTION_OUTCOMES_CLICKHOUSE_TABLE: &str = "execution_outcomes";

/// Extract execution outcomes from the StreamerMessage,
/// store execution outcomes in Clickhouse, and update the receipts cache with
/// mappings from receipt IDs to their parent transaction hashes.
/// This function processes only execution outcomes related to accounts of interest.
/// It uses the provided Clickhouse client for database operations and
/// a shared receipts cache to maintain the relationship between receipts and transactions.
///
/// Note: Execution Outcomes for Transactions are handled separately in `handlers/transactions.rs`.
pub async fn handle_execution_outcomes(
    message: &near_indexer_primitives::StreamerMessage,
    client: &clickhouse::Client,
    receipts_cache_arc: cache::ReceiptsCacheArc,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let execution_outcomes =
        extract_execution_outcomes(message, receipts_cache_arc.clone()).await?;

    if let Err(err) = crate::database::insert_rows(
        client,
        EXECUTION_OUTCOMES_CLICKHOUSE_TABLE,
        &execution_outcomes,
    )
    .await
    {
        crate::metrics::STORE_ERRORS_TOTAL.inc();
        tracing::error!("Error inserting rows into Clickhouse: {}", err);
        anyhow::bail!("Failed to insert rows into Clickhouse: {}", err)
    }
    tracing::debug!("handle_execution_outcomes {:?}", start.elapsed());
    Ok(())
}

// Extract execution outcomes from the StreamerMessage and update the receipts cache
async fn extract_execution_outcomes(
    message: &near_indexer_primitives::StreamerMessage,
    receipts_cache_arc: cache::ReceiptsCacheArc,
) -> anyhow::Result<Vec<types::ExecutionOutcomeRow>> {
    let start = Instant::now();
    let block_height = message.block.header.height;
    let block_timestamp = message.block.header.timestamp;
    let block_hash = message.block.header.hash.to_string();

    let execution_outcome_futures = message
        .shards
        .iter()
        .flat_map(|shard| shard.receipt_execution_outcomes.iter().cloned())
        .map(|outcome| {
            parse_execution_outcome(
                outcome,
                block_height,
                block_timestamp,
                block_hash.clone(),
                receipts_cache_arc.clone(),
            )
        });

    let execution_outcomes: Vec<types::ExecutionOutcomeRow> =
        futures::future::join_all(execution_outcome_futures)
            .await
            .into_iter()
            .flatten()
            .collect();

    // Add receipts that are going to be created by ExecutionOutcome to the cache (receipt-transaction matcher)
    tracing::debug!("Lock cache to set receipt-transaction mappings 1");
    let mut cache = receipts_cache_arc.write().await;
    for outcome in &execution_outcomes {
        for receipt_id in &outcome.receipt_ids {
            cache
                .set(
                    types::ReceiptOrDataId::ReceiptId(
                        near_primitives::hash::CryptoHash::from_str(receipt_id).unwrap_or_else(
                            |e| {
                                tracing::error!("Failed to parse receipt ID {}: {}", receipt_id, e);
                                near_primitives::hash::CryptoHash::default()
                            },
                        ),
                    ),
                    outcome.parent_transaction_hash.clone(),
                )
                .await;
        }
    }
    drop(cache);
    tracing::debug!("Released 1");

    // Potential cache
    tracing::debug!("Lock cache to set receipt-transaction mappings 2");
    let mut cache = receipts_cache_arc.write().await;
    for outcome in message
        .shards
        .iter()
        .flat_map(|shard| shard.receipt_execution_outcomes.iter())
    {
        if let Some(parent_tx_hash) = cache
            .potential_get(&types::ReceiptOrDataId::ReceiptId(
                outcome.execution_outcome.id,
            ))
            .await
        {
            for receipt_id in &outcome.execution_outcome.outcome.receipt_ids {
                let receipt_or_data_id = types::ReceiptOrDataId::ReceiptId(*receipt_id);
                if !execution_outcomes
                    .iter()
                    .any(|row| row.receipt_ids.contains(&receipt_id.to_string()))
                {
                    tracing::debug!("Adding to potential cache for receipt {}", receipt_id);
                    cache
                        .potential_set(receipt_or_data_id, parent_tx_hash.clone())
                        .await;
                } else {
                    tracing::debug!(
                        "No parent transaction hash found for receipt {}, skipping potential cache addition",
                        receipt_id
                    );
                }
            }
        }
    }
    drop(cache);
    tracing::debug!("Released 2");

    crate::metrics::ASSETS_IN_BLOCK_TOTAL
        .with_label_values(&["execution_outcomes"])
        .set(
            message
                .shards
                .iter()
                .map(|shard| shard.receipt_execution_outcomes.len() as i64)
                .sum(),
        );
    crate::metrics::ASSETS_IN_BLOCK_CAPTURED_TOTAL
        .with_label_values(&["execution_outcomes"])
        .set(execution_outcomes.len() as i64);
    tracing::debug!("extract_execution_outcomes {:?}", start.elapsed());
    Ok(execution_outcomes)
}

// Parse a receipt and update the receipts cache if it's related to accounts of interest
async fn parse_execution_outcome(
    outcome: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    block_height: u64,
    block_timestamp: u64,
    block_hash: String,
    receipts_cache_arc: cache::ReceiptsCacheArc,
) -> Option<types::ExecutionOutcomeRow> {
    let mut cache = receipts_cache_arc.write().await;
    if let Some(parent_tx_hash) = cache
        .get(&types::ReceiptOrDataId::ReceiptId(
            outcome.receipt.receipt_id,
        ))
        .await
    {
        Some(types::ExecutionOutcomeRow {
            block_height,
            block_timestamp,
            block_hash: block_hash.clone(),
            execution_outcome_id: outcome.execution_outcome.id.to_string(),
            parent_transaction_hash: parent_tx_hash.clone(),
            executor_id: outcome.execution_outcome.outcome.executor_id.to_string(),
            status: parse_status(outcome.execution_outcome.outcome.status.clone()),
            logs: serde_json::to_string(&outcome.execution_outcome.outcome.logs).unwrap_or_else(
                |e| {
                    tracing::error!("Failed to serialize logs: {}", e);
                    "[]".to_string()
                },
            ),
            tokens_burnt: outcome.execution_outcome.outcome.tokens_burnt.to_string(),
            gas_burnt: outcome.execution_outcome.outcome.gas_burnt,
            receipt_ids: outcome
                .execution_outcome
                .outcome
                .receipt_ids
                .iter()
                .map(|id| id.to_string())
                .collect(),
        })
    } else if super::any_account_id_of_interest(&[
        outcome.receipt.receiver_id.as_str(),
        outcome.receipt.predecessor_id.as_str(),
    ]) {
        tracing::info!(
            "We don't watch for the this execution outcome but it is related to the account ids of interest, {}",
            outcome.receipt.receipt_id
        );
        tracing::debug!("{:#?}", outcome.receipt);
        crate::metrics::POTENTIAL_ASSET_MISS_TOTAL
            .with_label_values(&["execution_outcomes"])
            .inc();

        // Try potential cache
        if let Some(parent_tx_hash) = cache
            .potential_get(&types::ReceiptOrDataId::ReceiptId(
                outcome.receipt.receipt_id,
            ))
            .await
        {
            // Promote this cache entry to the main cache
            receipts_cache_arc
                .write()
                .await
                .set(
                    types::ReceiptOrDataId::ReceiptId(outcome.receipt.receipt_id),
                    parent_tx_hash.clone(),
                )
                .await;

            tracing::info!(
                "Found a potential mapping for outcome {} to transaction {}",
                outcome.execution_outcome.id,
                parent_tx_hash
            );
            Some(types::ExecutionOutcomeRow {
                block_height,
                block_timestamp,
                block_hash: block_hash.clone(),
                execution_outcome_id: outcome.execution_outcome.id.to_string(),
                parent_transaction_hash: parent_tx_hash.clone(),
                executor_id: outcome.execution_outcome.outcome.executor_id.to_string(),
                status: parse_status(outcome.execution_outcome.outcome.status.clone()),
                logs: serde_json::to_string(&outcome.execution_outcome.outcome.logs)
                    .unwrap_or_else(|e| {
                        tracing::error!("Failed to serialize logs: {}", e);
                        "[]".to_string()
                    }),
                tokens_burnt: outcome.execution_outcome.outcome.tokens_burnt.to_string(),
                gas_burnt: outcome.execution_outcome.outcome.gas_burnt,
                receipt_ids: outcome
                    .execution_outcome
                    .outcome
                    .receipt_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect(),
            })
        } else {
            tracing::warn!(
                "No potential mapping found for outcome {}",
                outcome.execution_outcome.id
            );
            None
        }
    } else {
        None
    }
}
