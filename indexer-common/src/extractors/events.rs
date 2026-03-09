use blocksapi::near_indexer_primitives;

use crate::cache;
use crate::types::{EventJson, EventRow};
use futures::{StreamExt, stream};

pub const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";

/// Extract events from the StreamerMessage and resolve parent transaction hashes
/// via the receipts cache.
/// This function processes only events related to accounts of interest.
/// It uses the shared receipts cache to maintain the relationship between receipts and transactions.
#[tracing::instrument(
    name = "extract_events",
    skip(message, receipts_cache_arc),
    fields(block_height = message.block.header.height)
)]
pub async fn extract_events(
    message: &near_indexer_primitives::StreamerMessage,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    events_concurrency: usize,
    accounts_of_interest: &[String],
) -> anyhow::Result<Vec<EventRow>> {
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
                        accounts_of_interest,
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
    crate::metrics::ASSETS_IN_BLOCK_TOTAL
        .with_label_values(&["events"])
        .set(event_count as i64);

    let rows: Vec<EventRow> =
        event_results
            .into_iter()
            .try_fold(Vec::with_capacity(event_count), |mut acc, res| match res {
                Ok(Some(r)) => {
                    acc.push(r);
                    Ok(acc)
                }
                Ok(None) => Ok(acc),
                Err(err) => {
                    crate::metrics::STORE_ERRORS_TOTAL.inc();
                    tracing::error!(
                        target: crate::config::INDEXER,
                        error = %err,
                        "Failed parsing event row"
                    );
                    Err(err)
                }
            })?;

    crate::metrics::ASSETS_IN_BLOCK_CAPTURED_TOTAL
        .with_label_values(&["events"])
        .set(rows.len() as i64);

    Ok(rows)
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
    accounts_of_interest: &[String],
) -> anyhow::Result<Option<EventRow>> {
    // 0. Fast contract filter: drop immediately if executor not of interest.
    let contract_id_ref = outcome.execution_outcome.outcome.executor_id.as_str();
    if !accounts_of_interest.iter().any(|a| a == contract_id_ref) {
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
        Err(err) => {
            tracing::warn!(
                target: crate::config::INDEXER,
                error = %err,
                "Failed to deserialize event JSON, skipping"
            );
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
        Err(err) => {
            tracing::debug!(
                target: crate::config::INDEXER,
                receipt_id=%outcome.receipt.receipt_id,
                error=%err,
                "redis get failed for event (tx_hash omitted)"
            );
            None
        }
    };

    let accounts_refs: Vec<&str> = accounts_of_interest.iter().map(|s| s.as_str()).collect();
    if tx_hash.is_none()
        && crate::any_account_id_of_interest(
            &[
                outcome.receipt.receiver_id.as_str(),
                outcome.receipt.predecessor_id.as_str(),
            ],
            &accounts_refs,
        )
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
            Err(err) => {
                tracing::debug!(
                    target: crate::config::INDEXER,
                    receipt_id=%outcome.receipt.receipt_id,
                    error=%err,
                    "redis potential_get failed for event"
                );
            }
        }
    }

    let Some(tx_hash) = tx_hash else {
        tracing::warn!(
            target: crate::config::INDEXER,
            "Could not resolve parent tx hash for receipt_id: {}",
            outcome.receipt.receipt_id
        );
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
pub fn parse_status(status: near_indexer_primitives::views::ExecutionStatusView) -> String {
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
