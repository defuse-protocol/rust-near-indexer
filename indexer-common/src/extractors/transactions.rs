use blocksapi::near_indexer_primitives;

use crate::{cache, extractors::events::parse_status, types};
use tokio::try_join;

/// Extract transactions and their execution outcomes from the StreamerMessage,
/// updating the receipts cache with mappings from receipt IDs to their parent transaction hashes.
/// Returns (TransactionRows, ExecutionOutcomeRows).
#[tracing::instrument(
    name = "extract_transactions_and_outcomes",
    skip(message, receipts_cache_arc),
    fields(block_height = message.block.header.height)
)]
pub async fn extract_transactions(
    message: &near_indexer_primitives::StreamerMessage,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    accounts_of_interest: &[String],
) -> anyhow::Result<(Vec<types::TransactionRow>, Vec<types::ExecutionOutcomeRow>)> {
    try_join!(
        extract_transaction_rows(message, receipts_cache_arc.clone(), accounts_of_interest),
        extract_transaction_execution_outcomes(message, accounts_of_interest)
    )
}

/// Extract transaction rows from the StreamerMessage and update the receipts cache.
async fn extract_transaction_rows(
    message: &near_indexer_primitives::StreamerMessage,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    accounts_of_interest: &[String],
) -> anyhow::Result<Vec<types::TransactionRow>> {
    let block_height = message.block.header.height;
    let block_hash = message.block.header.hash.to_string();
    let block_timestamp = message.block.header.timestamp;

    let accounts_refs: Vec<&str> = accounts_of_interest.iter().map(|s| s.as_str()).collect();
    let transaction_futures = message
        .shards
        .iter()
        .filter_map(|shard| shard.chunk.as_ref())
        .flat_map(|chunk| chunk.transactions.iter().map(Clone::clone))
        .map(|tx| {
            parse_transaction(
                tx,
                block_height,
                block_timestamp,
                block_hash.clone(),
                receipts_cache_arc.clone(),
                &accounts_refs,
            )
        });

    let transactions: Vec<types::TransactionRow> = futures::future::join_all(transaction_futures)
        .await
        .into_iter()
        .flatten()
        .collect();

    crate::metrics::ASSETS_IN_BLOCK_TOTAL
        .with_label_values(&["transactions"])
        .set(
            message
                .shards
                .iter()
                .filter_map(|shard| shard.chunk.as_ref())
                .map(|chunk| chunk.transactions.len() as i64)
                .sum(),
        );
    crate::metrics::ASSETS_IN_BLOCK_CAPTURED_TOTAL
        .with_label_values(&["transactions"])
        .set(transactions.len() as i64);
    tracing::debug!(
        target: crate::config::INDEXER,
        "extract_transactions done"
    );
    Ok(transactions)
}

/// Parse a transaction and update the receipts cache if it's related to accounts of interest.
/// Also updates the potential cache for transactions not directly related to accounts of interest.
async fn parse_transaction(
    tx: near_indexer_primitives::IndexerTransactionWithOutcome,
    block_height: u64,
    block_timestamp: u64,
    block_hash: String,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    accounts_of_interest: &[&str],
) -> Option<types::TransactionRow> {
    let transaction_hash = tx.transaction.hash.to_string();
    let converted_into_receipt_id = tx
        .outcome
        .execution_outcome
        .outcome
        .receipt_ids
        .first()
        .expect("`receipt_ids` must contain one Receipt Id");

    if crate::any_account_id_of_interest(
        &[
            tx.transaction.signer_id.as_str(),
            tx.transaction.receiver_id.as_str(),
        ],
        accounts_of_interest,
    ) {
        // Save this Transaction hash to ReceiptsCache
        // we use the Receipt ID to which this transaction was converted
        // and the Transaction hash as a value.
        // Later, while Receipt will be looking for a parent Transaction hash
        // it will be able to find it in the ReceiptsCache
        receipts_cache_arc
            .set(
                types::ReceiptOrDataId::ReceiptId(*converted_into_receipt_id),
                transaction_hash.clone(),
            )
            .await;

        Some(types::TransactionRow {
            block_height,
            block_timestamp,
            block_hash: block_hash.clone(),
            transaction_hash,
            signer_id: tx.transaction.signer_id.to_string(),
            receiver_id: tx.transaction.receiver_id.to_string(),
            actions: serde_json::to_string(
                &tx.transaction
                    .actions
                    .iter()
                    .filter_map(|a| types::Action::try_from(a).ok())
                    .collect::<Vec<types::Action>>(),
            )
            .expect("Failed to serialize actions for transaction"),
        })
    } else {
        // We set a potential mapping in the potential cache
        // so that if we see a receipt related to an account of interest
        // we can still find the parent transaction hash
        tracing::debug!(
            target: crate::config::INDEXER,
            "Add receipt to potential cache: {}",
            converted_into_receipt_id,
        );
        receipts_cache_arc
            .potential_set(
                types::ReceiptOrDataId::ReceiptId(*converted_into_receipt_id),
                transaction_hash.clone(),
            )
            .await;
        None
    }
}

// Extract transaction execution outcomes from the StreamerMessage
// This is separate from receipt execution outcomes handled in receipts_and_outcomes.rs
async fn extract_transaction_execution_outcomes(
    message: &near_indexer_primitives::StreamerMessage,
    accounts_of_interest: &[String],
) -> anyhow::Result<Vec<types::ExecutionOutcomeRow>> {
    let block_height = message.block.header.height;
    let block_hash = message.block.header.hash.to_string();
    let block_timestamp = message.block.header.timestamp;

    let accounts_refs: Vec<&str> = accounts_of_interest.iter().map(|s| s.as_str()).collect();
    let execution_outcome_futures = message
        .shards
        .iter()
        .filter_map(|shard| shard.chunk.as_ref())
        .flat_map(|chunk| chunk.transactions.iter().map(Clone::clone))
        .map(|tx| {
            parse_transaction_execution_outcome(
                tx,
                block_height,
                block_timestamp,
                block_hash.clone(),
                &accounts_refs,
            )
        });

    let execution_outcomes: Vec<types::ExecutionOutcomeRow> =
        futures::future::join_all(execution_outcome_futures)
            .await
            .into_iter()
            .flatten()
            .collect();
    tracing::debug!(
        target: crate::config::INDEXER,
        "extract_transaction_execution_outcomes done"
    );
    Ok(execution_outcomes)
}

/// Parse a transaction execution outcome and return an ExecutionOutcomeRow if it's related to accounts of interest.
async fn parse_transaction_execution_outcome(
    tx: near_indexer_primitives::IndexerTransactionWithOutcome,
    block_height: u64,
    block_timestamp: u64,
    block_hash: String,
    accounts_of_interest: &[&str],
) -> Option<types::ExecutionOutcomeRow> {
    if crate::any_account_id_of_interest(
        &[
            tx.transaction.signer_id.as_str(),
            tx.transaction.receiver_id.as_str(),
        ],
        accounts_of_interest,
    ) {
        Some(types::ExecutionOutcomeRow {
            block_height,
            block_timestamp,
            block_hash: block_hash.clone(),
            execution_outcome_id: tx.outcome.execution_outcome.id.to_string(),
            executor_id: tx.outcome.execution_outcome.outcome.executor_id.to_string(),
            parent_transaction_hash: tx.transaction.hash.to_string(),
            status: parse_status(tx.outcome.execution_outcome.outcome.status.clone()),
            gas_burnt: tx.outcome.execution_outcome.outcome.gas_burnt.as_gas(),
            tokens_burnt: tx
                .outcome
                .execution_outcome
                .outcome
                .tokens_burnt
                .to_string(),
            logs: serde_json::to_string(&tx.outcome.execution_outcome.outcome.logs)
                .expect("Failed to serialize logs for transaction execution outcome"),
            receipt_ids: tx
                .outcome
                .execution_outcome
                .outcome
                .receipt_ids
                .iter()
                .map(|id| id.to_string().clone())
                .collect::<Vec<String>>(),
        })
    } else {
        None
    }
}
