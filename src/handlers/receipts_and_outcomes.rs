//! Single-pass receipt + execution outcome handler.
//!
//! Goal: iterate execution outcomes exactly once and during that traversal produce:
//!  * ExecutionOutcome rows (for outcomes whose originating receipt we can map to a tx)
//!  * Receipt rows derived from the embedded receipt in the same structure
//!    (avoids a second independent iteration over chunks/receipts)
//!
//! Cache strategy:
//!  * Main cache stores definitive mapping: receipt_or_data_id -> parent_tx_hash.
//!  * Potential cache holds speculative parent tx hashes for receipts that might become
//!    relevant; promotion happens if an outcome/receipt touches an account of interest.
//!  * While iterating an execution outcome we also immediately map all child receipt ids
//!    it spawns to the parent tx (eliminates follow-up pass previously required).
//!
//! What we intentionally dropped versus earlier multi-phase version:
//!  * Separate extraction functions for outcomes and receipts.
//!  * Post-pass cache population of potential mappings.
//!  * Per-element async fan-out (simple synchronous loop is cheaper & sufficient here).
//!
//! Observability:
//!  * A single span wraps collection; inserts still have individual spans.
//!  * Promotions / potential misses logged at info/debug for cache tuning.
//!
//! Metrics:
//!  * Totals/captured metrics preserved for continuity.
//!  * Potential misses still increment POTENTIAL_ASSET_MISS_TOTAL.
//!
//! Error handling:
//!  * DB insertion errors log and increment STORE_ERRORS_TOTAL; handler returns early.
//!
//! Keep inline comments focused on cache edge cases; names should explain everything else.

use blocksapi::near_indexer_primitives::{self, near_primitives};
use futures::StreamExt;
use tracing::Instrument;

use crate::{cache, handlers::events::parse_status, types};

pub(crate) const EXECUTION_OUTCOMES_CLICKHOUSE_TABLE: &str = "execution_outcomes";
pub(crate) const RECEIPTS_CLICKHOUSE_TABLE: &str = "receipts";

#[tracing::instrument(
    name = "handle_receipts_and_outcomes",
    skip(message, client, receipts_cache_arc),
    fields(block_height = message.block.header.height)
)]
pub async fn handle_receipts_and_outcomes(
    message: &near_indexer_primitives::StreamerMessage,
    client: &clickhouse::Client,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    outcome_concurrency: usize,
) -> anyhow::Result<()> {
    // Single pass: iterate execution outcomes once; build outcome rows and needed receipt rows.
    let single_pass_span = tracing::debug_span!("single_pass_collect");
    let (execution_outcomes, receipts) =
        collect_outcomes_and_receipts(message, receipts_cache_arc.clone(), outcome_concurrency)
            .instrument(single_pass_span)
            .await?;

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
        .set(receipts.len() as i64);

    // Insert both batches concurrently (skip empty to avoid pointless spans)
    let insert_span = tracing::debug_span!(
        "insert_batches",
        execution_outcomes_count = execution_outcomes.len(),
        receipts_count = receipts.len()
    );
    let exec_span = tracing::debug_span!(
        "insert_execution_outcomes_to_db",
        count = execution_outcomes.len()
    );
    let exec_task = async move {
        if execution_outcomes.is_empty() {
            return Ok::<_, anyhow::Error>(());
        }
        if let Err(e) = crate::database::insert_rows(
            client,
            EXECUTION_OUTCOMES_CLICKHOUSE_TABLE,
            &execution_outcomes,
        )
        .await
        {
            crate::metrics::STORE_ERRORS_TOTAL.inc();
            tracing::error!(error=%e, debug=?e, "Failed to insert execution outcomes");
            return Err(anyhow::anyhow!("execution_outcomes insert failed: {e}"));
        }
        Ok(())
    }
    .instrument(exec_span);

    let receipts_span = tracing::debug_span!("insert_receipts_to_db", count = receipts.len());
    let receipts_task = async move {
        if receipts.is_empty() {
            return Ok::<_, anyhow::Error>(());
        }
        if let Err(e) =
            crate::database::insert_rows(client, RECEIPTS_CLICKHOUSE_TABLE, &receipts).await
        {
            crate::metrics::STORE_ERRORS_TOTAL.inc();
            tracing::error!(error=%e, debug=?e, "Failed to insert receipts");
            return Err(anyhow::anyhow!("receipts insert failed: {e}"));
        }
        Ok(())
    }
    .instrument(receipts_span);

    if let Err(e) = (async { tokio::try_join!(exec_task, receipts_task) })
        .instrument(insert_span)
        .await
    {
        tracing::error!(error=%e, "Failed inserting batches");
        return Err(e);
    }

    tracing::debug!("handle_receipts_and_outcomes completed");
    Ok(())
}

// === Single-pass collection ===

#[tracing::instrument(
    name = "collect_outcomes_and_receipts",
    skip(message, receipts_cache_arc),
    fields(block_height = message.block.header.height)
)]
async fn collect_outcomes_and_receipts(
    message: &near_indexer_primitives::StreamerMessage,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    outcome_concurrency: usize,
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

    let mut stream = futures::stream::iter(all_outcomes.into_iter().map(|outcome| {
        let receipts_cache_arc = receipts_cache_arc.clone();
        let block_hash_clone = block_hash.clone();
        async move {
            let receipt_id = outcome.receipt.receipt_id;
            let iter_span = tracing::debug_span!(
                "outcome_iter",
                receipt_id = %receipt_id,
                outcome_id = %outcome.execution_outcome.id,
                receiver = %outcome.receipt.receiver_id,
                predecessor = %outcome.receipt.predecessor_id,
                child_receipts = outcome.execution_outcome.outcome.receipt_ids.len()
            );
            async {
                // cache lookup
                let mut parent_tx_opt = match receipts_cache_arc
                    .get(&types::ReceiptOrDataId::ReceiptId(receipt_id))
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::debug!(receipt_id=%receipt_id, error=%e, "redis get failed (treating as miss)");
                        None
                    }
                };
                if parent_tx_opt.is_none()
                    && super::any_account_id_of_interest(&[
                        outcome.receipt.receiver_id.as_str(),
                        outcome.receipt.predecessor_id.as_str(),
                    ])
                {
                    match receipts_cache_arc
                        .potential_get(&types::ReceiptOrDataId::ReceiptId(receipt_id))
                        .await
                    {
                        Ok(Some(p)) => {
                            receipts_cache_arc
                                .set(types::ReceiptOrDataId::ReceiptId(receipt_id), p.clone())
                                .await;
                            parent_tx_opt = Some(p.clone());
                            crate::metrics::PROMOTIONS_TOTAL
                                .with_label_values(&["execution_outcomes"])
                                .inc();
                        }
                        Ok(None) => {
                            crate::metrics::POTENTIAL_ASSET_MISS_TOTAL
                                .with_label_values(&["execution_outcomes"])
                                .inc();
                        }
                        Err(e) => {
                            tracing::warn!(receipt_id=%receipt_id, error=%e, "redis potential_get failed (skip miss metric)");
                        }
                    }
                }
                if let Some(parent_tx_hash) = parent_tx_opt {
                    let logs_json = {
                        let logs = &outcome.execution_outcome.outcome.logs;
                        if logs.is_empty() {
                            "[]".to_string()
                        } else {
                            serde_json::to_string(logs).unwrap_or_else(|e| {
                                tracing::error!(error=%e, "Failed to serialize logs");
                                "[]".to_string()
                            })
                        }
                    };
                    let receipt_ids: Vec<String> = outcome
                        .execution_outcome
                        .outcome
                        .receipt_ids
                        .iter()
                        .map(|id| id.to_string())
                        .collect();
                    let outcome_row = types::ExecutionOutcomeRow {
                        block_height,
                        block_timestamp,
                        block_hash: block_hash_clone.clone(),
                        execution_outcome_id: outcome.execution_outcome.id.to_string(),
                        parent_transaction_hash: parent_tx_hash.clone(),
                        executor_id: outcome.execution_outcome.outcome.executor_id.to_string(),
                        status: parse_status(outcome.execution_outcome.outcome.status.clone()),
                        logs: logs_json,
                        tokens_burnt: outcome.execution_outcome.outcome.tokens_burnt.to_string(),
                        gas_burnt: outcome.execution_outcome.outcome.gas_burnt,
                        receipt_ids,
                    };
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
                    // Receipt row
                    let r_view = &outcome.receipt;
                    let actions_json = match r_view.receipt {
                        near_primitives::views::ReceiptEnumView::Action { ref actions, .. } => {
                            serde_json::to_string(
                                &actions
                                    .iter()
                                    .flat_map(types::Action::try_from)
                                    .collect::<Vec<types::Action>>(),
                            )
                            .unwrap_or_else(|e| {
                                tracing::error!("Failed to serialize actions for receipt: {}", e);
                                "[]".to_string()
                            })
                        }
                        near_primitives::views::ReceiptEnumView::Data { ref data, .. } => {
                            serde_json::to_string(data).unwrap_or_else(|e| {
                                tracing::warn!("Failed to serialize receipt data: {}", e);
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
                        parent_transaction_hash: parent_tx_hash,
                        receipt_id: r_view.receipt_id.to_string(),
                        receiver_id: r_view.receiver_id.to_string(),
                        predecessor_id: r_view.predecessor_id.to_string(),
                        actions: actions_json,
                    };
                    Some((outcome_row, receipt_row))
                } else {
                    None
                }
            }
            .instrument(iter_span)
            .await
        }
    }))
    .buffer_unordered(outcome_concurrency);

    while let Some(res) = stream.next().await {
        if let Some((o, r)) = res {
            outcomes_rows.push(o);
            receipt_rows.push(r);
        }
    }

    tracing::debug!(
        outcomes = outcomes_rows.len(),
        receipts = receipt_rows.len(),
        "collect_outcomes_and_receipts built rows"
    );
    Ok((outcomes_rows, receipt_rows))
}
