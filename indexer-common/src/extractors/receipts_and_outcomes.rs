//! Single-pass receipt + execution outcome extractor.
//!
//! Goal: iterate execution outcomes exactly once and during that traversal produce:
//!  * ExecutionOutcome rows (for outcomes whose originating receipt we can map to a tx)
//!  * Receipt rows derived from the embedded receipt in the same structure
//!    (avoids a second independent iteration over chunks/receipts)
//!
//! Cache strategy:
//!  * Single keyspace: receipt_or_data_id -> parent_tx_hash. Every tx we see gets cached
//!    regardless of whether its receiver is in `accounts_of_interest` — bounded by TTL.
//!  * While iterating an execution outcome we also immediately map all child receipt ids
//!    it spawns to the parent tx (eliminates follow-up pass previously required).
//!
//! What we intentionally dropped versus earlier multi-phase version:
//!  * Separate extraction functions for outcomes and receipts.
//!  * Per-element async fan-out (simple synchronous loop is cheaper & sufficient here).
//!  * The earlier two-keyspace cache (main + potential, with promotion logic). A miss
//!    on the unified cache is now a real miss (typically a cross-contract chain whose
//!    originating tx was never delivered upstream); callers handle it by writing rows
//!    with NULL parent_transaction_hash, observable via ROWS_WITH_NULL_TX_HASH_TOTAL.
//!
//! Keep inline comments focused on cache edge cases; names should explain everything else.

use blocksapi::near_indexer_primitives::{self, near_primitives};
use futures::StreamExt;

use crate::{cache, extractors::events::parse_status, types};

/// Collect execution outcome and receipt rows from the StreamerMessage in a single pass.
/// Returns (ExecutionOutcomeRows, ReceiptRows).
#[tracing::instrument(
    name = "collect_outcomes_and_receipts",
    skip(message, receipts_cache_arc),
    fields(block_height = message.block.header.height)
)]
pub async fn collect_outcomes_and_receipts(
    message: &near_indexer_primitives::StreamerMessage,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    outcome_concurrency: usize,
    accounts_of_interest: &[String],
) -> anyhow::Result<(Vec<types::ExecutionOutcomeRow>, Vec<types::ReceiptRow>)> {
    let block_height = message.block.header.height;
    let block_timestamp = message.block.header.timestamp;
    let block_hash = message.block.header.hash.to_string();
    let estimated_outcomes: usize = message
        .shards
        .iter()
        .map(|s| s.receipt_execution_outcomes.len())
        .sum();
    let mut outcomes_rows = Vec::with_capacity(estimated_outcomes);
    let mut receipt_rows = Vec::with_capacity(estimated_outcomes); // heuristic

    // Parallelize per-outcome processing with controlled concurrency.
    let all_outcomes: Vec<&near_indexer_primitives::IndexerExecutionOutcomeWithReceipt> = message
        .shards
        .iter()
        .flat_map(|shard| shard.receipt_execution_outcomes.iter())
        .collect();

    let accounts_owned: Vec<String> = accounts_of_interest.to_vec();
    let mut stream = futures::stream::iter(all_outcomes.into_iter().map(|outcome| {
        let accounts = accounts_owned.clone();
        process_single_outcome(
            outcome,
            block_height,
            block_timestamp,
            block_hash.clone(),
            receipts_cache_arc.clone(),
            accounts,
        )
    }))
    .buffer_unordered(outcome_concurrency);

    while let Some(res) = stream.next().await {
        if let Some((o, r)) = res {
            outcomes_rows.push(o);
            receipt_rows.push(r);
        }
    }

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
        .set(outcomes_rows.len() as i64);
    crate::metrics::ASSETS_IN_BLOCK_TOTAL
        .with_label_values(&["receipts"])
        .set(
            message
                .shards
                .iter()
                .filter_map(|s| s.chunk.as_ref())
                .map(|c| c.receipts.len() as i64)
                .sum(),
        );
    crate::metrics::ASSETS_IN_BLOCK_CAPTURED_TOTAL
        .with_label_values(&["receipts"])
        .set(receipt_rows.len() as i64);

    tracing::debug!(
        target: crate::config::INDEXER,
        outcomes = outcomes_rows.len(),
        receipts = receipt_rows.len(),
        "collect_outcomes_and_receipts built rows"
    );
    Ok((outcomes_rows, receipt_rows))
}

async fn process_single_outcome(
    outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    block_height: u64,
    block_timestamp: u64,
    block_hash: String,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    accounts_of_interest: Vec<String>,
) -> Option<(types::ExecutionOutcomeRow, types::ReceiptRow)> {
    let receipts_cache_arc = receipts_cache_arc.clone();
    let block_hash_clone = block_hash.clone();

    let receipt_id = outcome.receipt.receipt_id;
    let accounts_refs: Vec<&str> = accounts_of_interest.iter().map(|s| s.as_str()).collect();

    let parent_tx_opt = find_parent_tx_hash(receipt_id, &receipts_cache_arc).await;

    // Whether this outcome/receipt is one we want to emit rows for. The cache-resolution
    // gate (parent_tx_opt) is independent: if the chain originated on an untracked account
    // BlocksAPI never delivered the originating tx, so we have no parent-tx mapping — but
    // the leaf receipt is still on a tracked account and the row is still useful. In that
    // case we write the row with parent_transaction_hash = NULL rather than dropping it.
    let of_interest = crate::any_account_id_of_interest(
        &[
            outcome.receipt.receiver_id.as_str(),
            outcome.receipt.predecessor_id.as_str(),
        ],
        &accounts_refs,
    );

    if of_interest {
        if parent_tx_opt.is_none() {
            tracing::warn!(
                target: crate::config::INDEXER,
                receipt_id = %receipt_id,
                "Could not resolve parent tx hash; writing outcome/receipt with NULL parent_transaction_hash"
            );
            crate::metrics::ROWS_WITH_NULL_TX_HASH_TOTAL
                .with_label_values(&["execution_outcomes"])
                .inc();
            crate::metrics::ROWS_WITH_NULL_TX_HASH_TOTAL
                .with_label_values(&["receipts"])
                .inc();
        }

        let logs_json = {
            let logs = &outcome.execution_outcome.outcome.logs;
            if logs.is_empty() {
                "[]".to_string()
            } else {
                serde_json::to_string(logs).unwrap_or_else(|err| {
                    tracing::error!(
                        target: crate::config::INDEXER,
                        error=%err,
                        "Failed to serialize logs"
                    );
                    "[]".to_string()
                })
            }
        };
        let receipt_ids: Vec<types::ReceiptOrDataId> = outcome
            .execution_outcome
            .outcome
            .receipt_ids
            .iter()
            .map(|id| types::ReceiptOrDataId::ReceiptId(*id))
            .collect();

        let outcome_row = types::ExecutionOutcomeRow {
            block_height,
            block_timestamp,
            block_hash: block_hash_clone.clone(),
            execution_outcome_id: outcome.execution_outcome.id.to_string(),
            parent_transaction_hash: parent_tx_opt.clone(),
            executor_id: outcome.execution_outcome.outcome.executor_id.to_string(),
            status: parse_status(outcome.execution_outcome.outcome.status.clone()),
            logs: logs_json,
            tokens_burnt: outcome.execution_outcome.outcome.tokens_burnt.to_string(),
            gas_burnt: outcome.execution_outcome.outcome.gas_burnt.as_gas(),
            receipt_ids: receipt_ids.iter().map(|id| id.to_string()).collect(),
        };

        // Only propagate the parent-tx mapping to children when we actually have one.
        if let Some(ref parent_tx_hash) = parent_tx_opt {
            receipts_cache_arc
                .set_many_receipts(receipt_ids, parent_tx_hash)
                .await;
        }

        // Receipt row
        let r_view = &outcome.receipt;
        let actions_json = match r_view.receipt {
            near_primitives::views::ReceiptEnumView::Action { ref actions, .. } => {
                serde_json::to_string(
                    &actions
                        .iter()
                        .filter_map(|a| types::Action::try_from(a).ok())
                        .collect::<Vec<types::Action>>(),
                )
                .unwrap_or_else(|err| {
                    tracing::error!(
                        target: crate::config::INDEXER,
                        "Failed to serialize actions for receipt: {}",
                        err
                    );
                    "[]".to_string()
                })
            }
            near_primitives::views::ReceiptEnumView::Data { ref data, .. } => {
                serde_json::to_string(data).unwrap_or_else(|err| {
                    tracing::warn!(
                        target: crate::config::INDEXER,
                        "Failed to serialize receipt data: {}",
                        err
                    );
                    "null".to_string()
                })
            }
            near_primitives::views::ReceiptEnumView::GlobalContractDistribution { .. } => {
                "".to_string()
            }
        };
        let receipt_row = types::ReceiptRow {
            block_height,
            block_timestamp,
            block_hash: block_hash_clone.clone(),
            parent_transaction_hash: parent_tx_opt,
            receipt_id: r_view.receipt_id.to_string(),
            receiver_id: r_view.receiver_id.to_string(),
            predecessor_id: r_view.predecessor_id.to_string(),
            actions: actions_json,
        };
        Some((outcome_row, receipt_row))
    } else {
        // Not-of-interest: don't emit rows, but still propagate the parent-tx mapping into
        // the unified receipts cache for any descendant receipts (only useful when we actually
        // resolved a parent — phantom mappings would just be noise).
        if let Some(parent_tx_hash) = parent_tx_opt {
            let child_ids: Vec<types::ReceiptOrDataId> = outcome
                .execution_outcome
                .outcome
                .receipt_ids
                .iter()
                .map(|c| types::ReceiptOrDataId::ReceiptId(*c))
                .collect();

            receipts_cache_arc
                .set_many_receipts(child_ids, &parent_tx_hash)
                .await;
        }
        None
    }
}

// === Cache lookup ===
// Single-keyspace lookup. The receipt cache is now unified: every tx -> receipt-id mapping
// the indexer sees is written to `receipt_cache:<id>` regardless of whether the receiver is
// in `accounts_of_interest`. A miss here means we genuinely don't have the parent — typical
// for cross-contract chains where the originating tx was never delivered to us by the
// upstream framework. Callers handle None by writing rows with NULL parent_transaction_hash.
async fn find_parent_tx_hash(
    receipt_id: near_primitives::hash::CryptoHash,
    receipts_cache_arc: &cache::ReceiptsCacheArc,
) -> Option<String> {
    match receipts_cache_arc
        .get(&types::ReceiptOrDataId::ReceiptId(receipt_id))
        .await
    {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(
                target: crate::config::INDEXER,
                receipt_id=%receipt_id,
                error=%err,
                "redis get failed (treating as miss)"
            );
            None
        }
    }
}
