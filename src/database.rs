use crate::config::AppConfig;
use clickhouse::{Client, Row};
use tokio_retry::{Retry, strategy::FixedInterval};

const SAVE_ATTEMPTS: usize = 20;

pub fn init_clickhouse_client(config: &AppConfig) -> Client {
    Client::default()
        .with_url(&config.clickhouse_url)
        .with_user(&config.clickhouse_user)
        .with_password(&config.clickhouse_password)
        .with_database(&config.clickhouse_database)
}

/// Gets the last stored block_height from transactions table
pub async fn get_last_height_transactions(
    client: &Client,
) -> Result<u64, clickhouse::error::Error> {
    tracing::info!(
        target: crate::config::INDEXER,
        "Fetching last indexed block height from ClickHouse..."
    );
    client
        .query("SELECT max(block_height) FROM transactions")
        .fetch_one::<u64>()
        .await
}

/// Gets the last stored block_height from events table
/// Should be used along with the `--events-only` mode
pub async fn get_last_height_events(client: &Client) -> Result<u64, clickhouse::error::Error> {
    tracing::info!(
        target: crate::config::INDEXER,
        "Fetching last indexed block height from ClickHouse..."
    );
    client
        .query("SELECT max(block_height) FROM transactions")
        .fetch_one::<u64>()
        .await
}

#[tracing::instrument(
    name = "database_insert",
    level = "debug",
    skip(client, rows),
    fields(table = table, rows_count = rows.len())
)]
pub async fn insert_rows(
    client: &Client,
    table: &str,
    rows: &[impl Row + serde::Serialize],
) -> anyhow::Result<()> {
    let retry_strategy = FixedInterval::from_millis(500).take(SAVE_ATTEMPTS);
    Retry::spawn(retry_strategy, || async {
        try_insert_rows(client, table, rows).await.map_err(|err| {
            crate::metrics::DATABASE_INSERT_RETRIES_TOTAL.inc();
            tracing::warn!(
                target: crate::config::INDEXER,
                "Failed to insert rows into {}: {}",
                table,
                err
            );
            err
        })
    })
    .await
}

#[tracing::instrument(
    name = "try_database_insert",
    level = "debug",
    skip(client, rows),
    fields(table = table, rows_count = rows.len())
)]
async fn try_insert_rows(
    client: &Client,
    table: &str,
    rows: &[impl Row + serde::Serialize],
) -> anyhow::Result<()> {
    tracing::debug!(
        target: crate::config::INDEXER,
        "Inserting {} rows into table {}",
        rows.len(),
        table
    );
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert(table)?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}
