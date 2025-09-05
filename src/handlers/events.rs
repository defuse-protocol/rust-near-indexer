use cached::Cached;

use near_lake_framework::near_indexer_primitives;

use crate::types::{EventJson, EventRow};
use crate::CONTRACT_ACCOUNT_IDS_OF_INTEREST;

const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";
const EVENT_CLICKHOUSE_TABLE: &str = "events";

/// Extract events from the StreamerMessage,
/// store events in Clickhouse, and update the receipts cache with
/// mappings from receipt IDs to their parent transaction hashes.
/// This function processes only events related to accounts of interest.
/// It uses the provided Clickhouse client for database operations and
/// a shared receipts cache to maintain the relationship between receipts and transactions.
pub async fn handle_events(
    message: &near_indexer_primitives::StreamerMessage,
    client: &clickhouse::Client,
    receipts_cache_arc: crate::types::ReceiptsCacheArc,
) -> anyhow::Result<()> {
    let mut receipts_cache_lock = receipts_cache_arc.lock().await;
    let rows: Vec<EventRow> = message
        .shards
        .iter()
        .flat_map(|shard| shard.receipt_execution_outcomes.iter())
        .map(|outcome| {
            let parent_tx_hash = receipts_cache_lock.cache_get(
                &crate::types::ReceiptOrDataId::ReceiptId(outcome.receipt.receipt_id),
            );
            let tx_hash = parent_tx_hash.cloned();
            (tx_hash, outcome)
        })
        .flat_map(|(tx_hash, outcome)| {
            outcome
                .execution_outcome
                .outcome
                .logs
                .iter()
                .enumerate()
                .filter_map(move |(index_in_log, log)| {
                    parse_event(
                        index_in_log,
                        log,
                        outcome,
                        &message.block.header,
                        tx_hash.clone(),
                    )
                })
        })
        .collect();

    drop(receipts_cache_lock);
    if let Err(err) = crate::database::insert_rows(client, EVENT_CLICKHOUSE_TABLE, &rows).await {
        eprintln!("Error inserting rows into Clickhouse: {}", err);
        anyhow::bail!("Failed to insert rows into Clickhouse: {}", err)
    }
    Ok(())
}

/// Parse a log entry to extract an EventRow if it contains valid event data.
/// Returns Some(EventRow) if the log contains a valid event, otherwise None.
fn parse_event(
    index_in_log: usize,
    log: &str,
    outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    header: &near_indexer_primitives::views::BlockHeaderView,
    parent_tx_hash: Option<String>,
) -> Option<EventRow> {
    let log_trimmed = log.trim();

    if log_trimmed.starts_with(EVENT_JSON_PREFIX) {
        if let Ok(event) =
            serde_json::from_str::<EventJson>(&log_trimmed[EVENT_JSON_PREFIX.len()..])
        {
            let contract_id = &outcome.execution_outcome.outcome.executor_id.to_string();
            if CONTRACT_ACCOUNT_IDS_OF_INTEREST.contains(&contract_id.as_str()) {
                if log_trimmed.contains("dip4") || log_trimmed.contains("nep245") {
                    // println!("Event: {}", log_trimmed);
                    return Some(EventRow {
                        block_height: header.height,
                        block_timestamp: header.timestamp,
                        block_hash: header.hash.to_string(),
                        contract_id: contract_id.to_string(),
                        execution_status: parse_status(
                            outcome.execution_outcome.outcome.status.clone(),
                        ),
                        version: event.version,
                        standard: event.standard,
                        index_in_log: index_in_log as u64,
                        event: event.event,
                        data: event.data.to_string(),
                        related_receipt_id: outcome.receipt.receipt_id.to_string(),
                        related_receipt_receiver_id: outcome.receipt.receiver_id.to_string(),
                        related_receipt_predecessor_id: outcome.receipt.predecessor_id.to_string(),
                        tx_hash: parent_tx_hash,
                    });
                }
            }
        }
    }
    None
}

/// Helper to parse the execution status into a string representation.
pub(crate) fn parse_status(status: near_indexer_primitives::views::ExecutionStatusView) -> String {
    match status {
        near_indexer_primitives::views::ExecutionStatusView::SuccessReceiptId(_) => {
            "success_receipt_id".to_string()
        }
        near_indexer_primitives::views::ExecutionStatusView::SuccessValue(_) => {
            "success_value".to_string()
        }
        near_indexer_primitives::views::ExecutionStatusView::Unknown => "unknown".to_string(),
        near_indexer_primitives::views::ExecutionStatusView::Failure(_) => "failure".to_string(),
    }
}
