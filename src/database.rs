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

pub async fn get_last_height(client: &Client) -> Result<u64, clickhouse::error::Error> {
    tracing::info!("Fetching last indexed block height from ClickHouse...");
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
        try_insert_rows(client, table, rows).await.map_err(|e| {
            tracing::warn!("Failed to insert rows into {}: {}", table, e);
            e
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
    tracing::debug!("Inserting {} rows into table {}", rows.len(), table);
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
