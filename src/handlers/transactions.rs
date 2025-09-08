use std::time::Instant;

use near_lake_framework::near_indexer_primitives;

use crate::{cache, handlers::events::parse_status, types};
use tokio::try_join;

const TRANSACTIONS_CLICKHOUSE_TABLE: &str = "transactions";

/// Extract transactions and their execution outcomes from the StreamerMessage,
/// store transactions and transaction execution outcomes in Clickhouse, and update the receipts cache with
/// mappings from receipt IDs to their parent transaction hashes.
/// This function processes only transactions related to accounts of interest.
/// It uses the provided Clickhouse client for database operations and
/// a shared receipts cache to maintain the relationship between receipts and transactions.
///
/// Note: Execution Outcomes for Receipts are handled separately in `handlers/execution_outcomes.rs`.
pub async fn handle_transactions(
    message: &near_indexer_primitives::StreamerMessage,
    client: &clickhouse::Client,
    receipts_cache_arc: cache::ReceiptsCacheArc,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let (transactions, transaction_execution_outcomes) = try_join!(
        extract_transactions(message, receipts_cache_arc.clone()),
        extract_transaction_execution_outcomes(message)
    )?;

    let result = match try_join!(
        crate::database::insert_rows(client, TRANSACTIONS_CLICKHOUSE_TABLE, &transactions,),
        crate::database::insert_rows(
            client,
            super::execution_outcomes::EXECUTION_OUTCOMES_CLICKHOUSE_TABLE,
            &transaction_execution_outcomes,
        )
    ) {
        Ok((_res_transactions, _res_execution_outcomes)) => Ok(()),
        Err(err) => {
            crate::metrics::STORE_ERRORS_TOTAL.inc();
            tracing::error!("Error during try_join for database inserts: {}", err);
            anyhow::bail!(
                "Failed to insert transactions or execution outcomes into Clickhouse: {}",
                err
            )
        }
    };
    tracing::info!("handle_transactions {:?}", start.elapsed());
    result
}

async fn extract_transactions(
    message: &near_indexer_primitives::StreamerMessage,
    receipts_cache_arc: cache::ReceiptsCacheArc,
) -> anyhow::Result<Vec<types::TransactionRow>> {
    let start = Instant::now();
    let receipts_cache_lock = receipts_cache_arc.lock().await;
    let block_height = message.block.header.height;
    let block_hash = message.block.header.hash.to_string();
    let block_timestamp = message.block.header.timestamp;

    let transactions = message
        .shards
        .iter()
        .filter_map(|shard| shard.chunk.as_ref())
        .flat_map(|chunk| chunk.transactions.iter().map(Clone::clone))
        .filter_map(|tx| {
            let transaction_hash = tx.transaction.hash.to_string();
            let converted_into_receipt_id = tx
                .outcome
                .execution_outcome
                .outcome
                .receipt_ids
                .first()
                .expect("`receipt_ids` must contain one Receipt Id");

            if super::any_account_id_of_interest(&[
                tx.transaction.signer_id.as_str(),
                tx.transaction.receiver_id.as_str(),
            ]) {
                // Save this Transaction hash to ReceiptsCache
                // we use the Receipt ID to which this transaction was converted
                // and the Transaction hash as a value.
                // Later, while Receipt will be looking for a parent Transaction hash
                // it will be able to find it in the ReceiptsCache
                receipts_cache_lock.set(
                    types::ReceiptOrDataId::ReceiptId(*converted_into_receipt_id),
                    transaction_hash.clone(),
                );
                Some(types::TransactionRow {
                    block_height: block_height,
                    block_timestamp: block_timestamp,
                    block_hash: block_hash.clone(),
                    transaction_hash: transaction_hash,
                    signer_id: tx.transaction.signer_id.to_string(),
                    receiver_id: tx.transaction.receiver_id.to_string(),
                    actions: serde_json::to_string(
                        &tx.transaction
                            .actions
                            .iter()
                            .map(|action| types::Action::from(action))
                            .collect::<Vec<types::Action>>(),
                    )
                    .expect("Failed to serialize actions for transaction"),
                })
            } else {
                // We set a potential mapping in the potential cache
                // so that if we see a receipt related to an account of interest
                // we can still find the parent transaction hash
                tracing::debug!(
                    "Add receipt to potential cache: {}",
                    converted_into_receipt_id,
                );
                receipts_cache_lock.potential_set(
                    types::ReceiptOrDataId::ReceiptId(*converted_into_receipt_id),
                    transaction_hash.clone(),
                );
                None
            }
        })
        .collect::<Vec<_>>();

    drop(receipts_cache_lock);

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
    tracing::info!("extract_transactions {:?}", start.elapsed());
    Ok(transactions)
}

async fn extract_transaction_execution_outcomes(
    message: &near_indexer_primitives::StreamerMessage,
) -> anyhow::Result<Vec<types::ExecutionOutcomeRow>> {
    let start = Instant::now();
    let block_height = message.block.header.height;
    let block_hash = message.block.header.hash.to_string();
    let block_timestamp = message.block.header.timestamp;

    let execution_outcomes = message
        .shards
        .iter()
        .filter_map(|shard| shard.chunk.as_ref())
        .flat_map(|chunk| chunk.transactions.iter().map(Clone::clone))
        .filter_map(|tx| {
            if super::any_account_id_of_interest(&[
                tx.transaction.signer_id.as_str(),
                tx.transaction.receiver_id.as_str(),
            ]) {
                Some(types::ExecutionOutcomeRow {
                    block_height: block_height,
                    block_timestamp: block_timestamp,
                    block_hash: block_hash.clone(),
                    execution_outcome_id: tx.outcome.execution_outcome.id.to_string(),
                    executor_id: tx.outcome.execution_outcome.outcome.executor_id.to_string(),
                    parent_transaction_hash: tx.transaction.hash.to_string(),
                    status: parse_status(tx.outcome.execution_outcome.outcome.status.clone()),
                    gas_burnt: tx.outcome.execution_outcome.outcome.gas_burnt,
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
        })
        .collect::<Vec<_>>();
    tracing::info!(
        "extract_transaction_execution_outcomes {:?}",
        start.elapsed()
    );
    Ok(execution_outcomes)
}
