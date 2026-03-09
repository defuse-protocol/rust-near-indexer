use blocksapi::near_indexer_primitives;

use indexer_common::cache;

const TRANSACTIONS_CLICKHOUSE_TABLE: &str = "transactions";
const EXECUTION_OUTCOMES_CLICKHOUSE_TABLE: &str = "execution_outcomes";

#[tracing::instrument(
    name = "handle_transactions",
    skip(message, client, receipts_cache_arc),
    fields(block_height = message.block.header.height)
)]
pub async fn handle_transactions(
    message: &near_indexer_primitives::StreamerMessage,
    client: &clickhouse::Client,
    receipts_cache_arc: cache::ReceiptsCacheArc,
    accounts_of_interest: &[String],
) -> anyhow::Result<()> {
    let (transactions, transaction_execution_outcomes) =
        indexer_common::extractors::transactions::extract_transactions(
            message,
            receipts_cache_arc,
            accounts_of_interest,
        )
        .await?;

    let result = {
        let _span = tracing::debug_span!(
            "insert_transactions_to_db",
            transactions_count = transactions.len(),
            execution_outcomes_count = transaction_execution_outcomes.len()
        )
        .entered();

        match tokio::try_join!(
            crate::database::insert_rows(client, TRANSACTIONS_CLICKHOUSE_TABLE, &transactions,),
            crate::database::insert_rows(
                client,
                EXECUTION_OUTCOMES_CLICKHOUSE_TABLE,
                &transaction_execution_outcomes,
            )
        ) {
            Ok((_res_transactions, _res_execution_outcomes)) => Ok(()),
            Err(err) => {
                indexer_common::metrics::STORE_ERRORS_TOTAL.inc();
                tracing::error!(
                    target: indexer_common::config::INDEXER,
                    "Error during try_join for database inserts: {}",
                    err
                );
                anyhow::bail!(
                    "Failed to insert transactions or execution outcomes into Clickhouse: {}",
                    err
                )
            }
        }
    };

    tracing::debug!(
        target: indexer_common::config::INDEXER,
        "handle_transactions completed"
    );
    result
}
