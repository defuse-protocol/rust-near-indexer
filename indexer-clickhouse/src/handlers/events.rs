use std::time::Instant;

use blocksapi::near_indexer_primitives;

use indexer_common::cache;

const EVENT_CLICKHOUSE_TABLE: &str = "events";

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

    let rows = indexer_common::extractors::events::extract_events(
        message,
        receipts_cache_arc,
        events_concurrency,
    )
    .await?;

    {
        let _span =
            tracing::debug_span!("insert_events_to_db", events_count = rows.len()).entered();
        if let Err(err) = crate::database::insert_rows(client, EVENT_CLICKHOUSE_TABLE, &rows).await
        {
            indexer_common::metrics::STORE_ERRORS_TOTAL.inc();
            tracing::error!(
                target: indexer_common::config::INDEXER,
                "Error inserting rows into Clickhouse: {}",
                err
            );
            anyhow::bail!("Failed to insert rows into Clickhouse: {}", err)
        }
    }

    let duration = start.elapsed();
    tracing::debug!(
        target: indexer_common::config::INDEXER,
        duration_ms = duration.as_millis(),
        "handle_events completed"
    );
    Ok(())
}
