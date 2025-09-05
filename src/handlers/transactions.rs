use cached::Cached;

use near_lake_framework::near_indexer_primitives;

use crate::types;
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
    receipts_cache_arc: types::ReceiptsCacheArc,
) -> anyhow::Result<()> {
    let (transactions, transaction_execution_outcomes) = try_join!(
        extract_transactions(message, receipts_cache_arc.clone()),
        extract_transaction_execution_outcomes(message)
    )?;

    match try_join!(
        crate::database::insert_rows(client, TRANSACTIONS_CLICKHOUSE_TABLE, &transactions,),
        crate::database::insert_rows(
            client,
            "transaction_execution_outcomes",
            &transaction_execution_outcomes,
        )
    ) {
        Ok((_res_transactions, _res_execution_outcomes)) => Ok(()),
        Err(err) => {
            eprintln!("Error during try_join for database inserts: {}", err);
            anyhow::bail!(
                "Failed to insert transactions or execution outcomes into Clickhouse: {}",
                err
            )
        }
    }
}

async fn extract_transactions(
    message: &near_indexer_primitives::StreamerMessage,
    receipts_cache_arc: types::ReceiptsCacheArc,
) -> anyhow::Result<Vec<types::TransactionRow>> {
    let mut receipts_cache_lock = receipts_cache_arc.lock().await;
    let block_height = message.block.header.height;
    let block_hash = message.block.header.hash.to_string();
    let block_timestamp = message.block.header.timestamp;

    let transactions = message
        .shards
        .iter()
        .filter_map(|shard| shard.chunk.as_ref())
        .flat_map(|chunk| chunk.transactions.iter().map(Clone::clone))
        .filter_map(|tx| {
            if super::any_account_id_of_interest(&[
                tx.transaction.signer_id.as_str(),
                tx.transaction.receiver_id.as_str(),
            ]) {
                let transaction_hash = tx.transaction.hash.to_string();
                let converted_into_receipt_id = tx
                    .outcome
                    .execution_outcome
                    .outcome
                    .receipt_ids
                    .first()
                    .expect("`receipt_ids` must contain one Receipt Id");

                // Save this Transaction hash to ReceiptsCache
                // we use the Receipt ID to which this transaction was converted
                // and the Transaction hash as a value.
                // Later, while Receipt will be looking for a parent Transaction hash
                // it will be able to find it in the ReceiptsCache
                receipts_cache_lock.cache_set(
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
                    .unwrap(),
                })
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    drop(receipts_cache_lock);

    println!(
        "intents.near transactions in block: {}, {:#?}",
        transactions.len(),
        transactions
            .iter()
            .map(|tx| tx.transaction_hash.to_string())
            .collect::<Vec<_>>()
    );
    Ok(transactions)
}

async fn extract_transaction_execution_outcomes(
    message: &near_indexer_primitives::StreamerMessage,
) -> anyhow::Result<Vec<types::ExecutionOutcomeRow>> {
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
                    status: serde_json::to_string(&tx.outcome.execution_outcome.outcome.status)
                        .unwrap(),
                    gas_burnt: tx.outcome.execution_outcome.outcome.gas_burnt,
                    tokens_burnt: tx
                        .outcome
                        .execution_outcome
                        .outcome
                        .tokens_burnt
                        .to_string(),
                    logs: serde_json::to_string(&tx.outcome.execution_outcome.outcome.logs)
                        .unwrap(),
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

    println!(
        "intents.near transaction execution outcomes in block: {}, {:#?}",
        execution_outcomes.len(),
        execution_outcomes
            .iter()
            .map(|outcome| &outcome.execution_outcome_id)
            .collect::<Vec<_>>()
    );
    Ok(execution_outcomes)
}
