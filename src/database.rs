use crate::config::AppConfig;
use clickhouse::{Client, Row};

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
    let mut insert = client.insert(table)?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}
