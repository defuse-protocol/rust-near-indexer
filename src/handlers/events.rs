use std::time::Instant;

use blocksapi_rs::near_indexer_primitives;

use crate::CONTRACT_ACCOUNT_IDS_OF_INTEREST;
use crate::cache;
use crate::types::{EventJson, EventRow};
use futures::{StreamExt, stream};

const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";
const EVENT_CLICKHOUSE_TABLE: &str = "events";

/// Extract events from the StreamerMessage,
/// store events in Clickhouse, and update the receipts cache with
/// mappings from receipt IDs to their parent transaction hashes.
/// This function processes only events related to accounts of interest.
/// It uses the provided Clickhouse client for database operations and
/// a shared receipts cache to maintain the relationship between receipts and transactions.
#[tracing::instrument(
    name = "handle_events",
    skip(message, client, receipts_cache_arc),
    fields(block_height = message.block.header.height)
)]
pub async fn handle_events(
    message: &near_indexer_primitives::StreamerMessage,
    client: &clickhouse::Client,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    events_concurrency: usize,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let event_futures: Vec<_> = message
        .shards
        .iter()
        .flat_map(|shard| shard.receipt_execution_outcomes.iter())
        .enumerate()
        .flat_map(|(index, outcome)| {
            outcome
                .execution_outcome
                .outcome
                .logs
                .iter()
                .enumerate()
                .map(|(index_in_log, log)| {
                    parse_event(
                        index_in_log,
                        log,
                        outcome,
                        &message.block.header,
                        index as u64,
                        receipts_cache_arc.clone(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();

    let event_results = {
        let _span =
            tracing::debug_span!("parse_events", events_count = event_futures.len()).entered();
        stream::iter(event_futures)
            .buffer_unordered(events_concurrency)
            .collect::<Vec<_>>()
            .await
    };

    let event_count = event_results.len();
    let rows: Vec<EventRow> =
        event_results
            .into_iter()
            .try_fold(Vec::with_capacity(event_count), |mut acc, res| match res {
                Ok(Some(r)) => {
                    acc.push(r);
                    Ok(acc)
                }
                Ok(None) => Ok(acc),
                Err(e) => {
                    crate::metrics::STORE_ERRORS_TOTAL.inc();
                    tracing::error!(error = %e, "Failed parsing event row");
                    Err(e)
                }
            })?;

    {
        let _span =
            tracing::debug_span!("insert_events_to_db", events_count = rows.len()).entered();
        if let Err(err) = crate::database::insert_rows(client, EVENT_CLICKHOUSE_TABLE, &rows).await
        {
            crate::metrics::STORE_ERRORS_TOTAL.inc();
            tracing::error!("Error inserting rows into Clickhouse: {}", err);
            anyhow::bail!("Failed to insert rows into Clickhouse: {}", err)
        }
    }

    let duration = start.elapsed();
    tracing::debug!(
        duration_ms = duration.as_millis(),
        "handle_events completed"
    );
    Ok(())
}

/// Parse a log entry to extract an EventRow if it contains valid event data.
/// Returns Some(EventRow) if the log contains a valid event, otherwise None.
#[tracing::instrument(
    name = "parse_event",
    skip(log, outcome, header, receipts_cache_arc, receipt_index_in_block),
    fields(
        block_height = header.height,
        block_timestamp = header.timestamp,
        block_hash = %header.hash
    )
)]
async fn parse_event(
    index_in_log: usize,
    log: &str,
    outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    header: &near_indexer_primitives::views::BlockHeaderView,
    receipt_index_in_block: u64,
    receipts_cache_arc: cache::ReceiptsCacheArc,
) -> anyhow::Result<Option<EventRow>> {
    // 0. Fast contract filter: drop immediately if executor not of interest.
    let contract_id_ref = outcome.execution_outcome.outcome.executor_id.as_str();
    if !CONTRACT_ACCOUNT_IDS_OF_INTEREST.contains(&contract_id_ref) {
        return Ok(None);
    }

    let log_trimmed = log.trim();

    // 1. Quick prefix check
    let Some(low_stripped) = log_trimmed.strip_prefix(EVENT_JSON_PREFIX) else {
        return Ok(None);
    };

    // 2. Parse JSON early
    let event: EventJson = match serde_json::from_str::<EventJson>(low_stripped) {
        Ok(ev) => ev,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to deserialize event JSON, skipping");
            return Ok(None);
        }
    };

    // 3. Standard / family filter
    if !(log_trimmed.contains("dip4") || log_trimmed.contains("nep245")) {
        return Ok(None);
    }

    // 4. Resolve parent tx hash (only now do cache lookups)
    let mut tx_hash = match receipts_cache_arc
        .get(&crate::types::ReceiptOrDataId::ReceiptId(
            outcome.receipt.receipt_id,
        ))
        .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(receipt_id=%outcome.receipt.receipt_id, error=%e, "redis get failed for event (tx_hash omitted)");
            None
        }
    };

    if tx_hash.is_none()
        && super::any_account_id_of_interest(&[
            outcome.receipt.receiver_id.as_str(),
            outcome.receipt.predecessor_id.as_str(),
        ])
    {
        match receipts_cache_arc
            .potential_get(&crate::types::ReceiptOrDataId::ReceiptId(
                outcome.receipt.receipt_id,
            ))
            .await
        {
            Ok(Some(parent)) => {
                receipts_cache_arc
                    .set(
                        crate::types::ReceiptOrDataId::ReceiptId(outcome.receipt.receipt_id),
                        parent.clone(),
                    )
                    .await;
                crate::metrics::PROMOTIONS_TOTAL
                    .with_label_values(&["events"])
                    .inc();
                tx_hash = Some(parent);
            }
            Ok(None) => {
                crate::metrics::POTENTIAL_ASSET_MISS_TOTAL
                    .with_label_values(&["events"])
                    .inc();
            }
            Err(e) => {
                tracing::debug!(receipt_id=%outcome.receipt.receipt_id, error=%e, "redis potential_get failed for event");
            }
        }
    }

    let Some(tx_hash) = tx_hash else {
        return Ok(None);
    };

    Ok(Some(EventRow {
        block_height: header.height,
        block_timestamp: header.timestamp,
        block_hash: header.hash.to_string(),
        contract_id: contract_id_ref.to_string(),
        execution_status: parse_status(outcome.execution_outcome.outcome.status.clone()),
        version: event.version,
        standard: event.standard,
        index_in_log: index_in_log as u64,
        event: event.event,
        data: event.data.to_string(),
        related_receipt_id: outcome.receipt.receipt_id.to_string(),
        related_receipt_receiver_id: outcome.receipt.receiver_id.to_string(),
        related_receipt_predecessor_id: outcome.receipt.predecessor_id.to_string(),
        tx_hash: Some(tx_hash),
        receipt_index_in_block,
    }))
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
