use blocksapi::near_indexer_primitives;
use tracing::Instrument;

use indexer_common::cache;

const EXECUTION_OUTCOMES_CLICKHOUSE_TABLE: &str = "execution_outcomes";
const RECEIPTS_CLICKHOUSE_TABLE: &str = "receipts";

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
    let (execution_outcomes, receipts) =
        indexer_common::extractors::receipts_and_outcomes::collect_outcomes_and_receipts(
            message,
            receipts_cache_arc,
            outcome_concurrency,
        )
        .await?;

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
        if let Err(err) = crate::database::insert_rows(
            client,
            EXECUTION_OUTCOMES_CLICKHOUSE_TABLE,
            &execution_outcomes,
        )
        .await
        {
            indexer_common::metrics::STORE_ERRORS_TOTAL.inc();
            tracing::error!(
                target: indexer_common::config::INDEXER,
                error=%err,
                debug=?err,
                "Failed to insert execution outcomes"
            );
            return Err(anyhow::anyhow!("execution_outcomes insert failed: {err}"));
        }
        Ok(())
    }
    .instrument(exec_span);

    let receipts_span = tracing::debug_span!("insert_receipts_to_db", count = receipts.len());
    let receipts_task = async move {
        if receipts.is_empty() {
            return Ok::<_, anyhow::Error>(());
        }
        if let Err(err) =
            crate::database::insert_rows(client, RECEIPTS_CLICKHOUSE_TABLE, &receipts).await
        {
            indexer_common::metrics::STORE_ERRORS_TOTAL.inc();
            tracing::error!(
                target: indexer_common::config::INDEXER,
                error=%err,
                debug=?err,
                "Failed to insert receipts"
            );
            return Err(anyhow::anyhow!("receipts insert failed: {err}"));
        }
        Ok(())
    }
    .instrument(receipts_span);

    if let Err(err) = (async { tokio::try_join!(exec_task, receipts_task) })
        .instrument(insert_span)
        .await
    {
        tracing::error!(
            target: indexer_common::config::INDEXER,
            error=%err,
            "Failed inserting batches"
        );
        return Err(err);
    }

    tracing::debug!(
        target: indexer_common::config::INDEXER,
        "handle_receipts_and_outcomes completed"
    );
    Ok(())
}
