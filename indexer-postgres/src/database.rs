use chrono::{DateTime, Utc};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use indexer_primitives::SilverDip4TransferRow;

const SAVE_ATTEMPTS: usize = 10;

pub async fn init_pg_pool(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    tracing::info!(
        target: indexer_common::config::INDEXER,
        "Connected to PostgreSQL, running migrations..."
    );

    sqlx::migrate!("./migrations").run(&pool).await?;

    tracing::info!(
        target: indexer_common::config::INDEXER,
        "PostgreSQL migrations applied"
    );

    Ok(pool)
}

pub async fn get_last_block_height(pool: &PgPool) -> anyhow::Result<u64> {
    let row: (i64,) =
        sqlx::query_as("SELECT COALESCE(MAX(block_height), 0) FROM silver_dip4_transfers")
            .fetch_one(pool)
            .await?;
    Ok(row.0 as u64)
}

pub async fn insert_transfer_rows(
    pool: &PgPool,
    rows: &[SilverDip4TransferRow],
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    // Exponential backoff: 250ms base, doubles up to 60s max, 10 attempts
    let retry_strategy = tokio_retry::strategy::ExponentialBackoff::from_millis(250)
        .max_delay(std::time::Duration::from_secs(60))
        .take(SAVE_ATTEMPTS);

    tokio_retry::Retry::spawn(retry_strategy, || async {
        try_insert_rows(pool, rows).await.map_err(|err| {
            indexer_common::metrics::DATABASE_INSERT_RETRIES_TOTAL.inc();
            tracing::warn!(
                target: indexer_common::config::INDEXER,
                "Failed to insert rows into PostgreSQL: {}",
                err
            );
            err
        })
    })
    .await
}

async fn try_insert_rows(
    pool: &PgPool,
    rows: &[SilverDip4TransferRow],
) -> anyhow::Result<()> {
    tracing::debug!(
        target: indexer_common::config::INDEXER,
        "Inserting {} rows into silver_dip4_transfers",
        rows.len()
    );

    let mut tx = pool.begin().await?;

    for row in rows {
        let amount: Option<sqlx::types::BigDecimal> = row
            .amount
            .parse::<sqlx::types::BigDecimal>()
            .ok();

        // Convert nanosecond epoch to TIMESTAMPTZ
        let secs = (row.block_timestamp / 1_000_000_000) as i64;
        let nanos = (row.block_timestamp % 1_000_000_000) as u32;
        let block_ts: DateTime<Utc> = DateTime::from_timestamp(secs, nanos)
            .unwrap_or_default();

        sqlx::query(
            r#"INSERT INTO silver_dip4_transfers (
                block_height, block_timestamp, block_hash, tx_hash,
                contract_id, execution_status, version, standard, event,
                related_receipt_id, related_receipt_receiver_id, related_receipt_predecessor_id,
                memo, old_owner_id, new_owner_id, token_id, amount,
                intent_hash, referral
            ) VALUES (
                $1, $2, $3, $4,
                $5, $6, $7, $8, $9,
                $10, $11, $12,
                $13, $14, $15, $16, $17,
                $18, $19
            ) ON CONFLICT DO NOTHING"#,
        )
        .bind(row.block_height as i64)
        .bind(block_ts)
        .bind(&row.block_hash)
        .bind(&row.tx_hash)
        .bind(&row.contract_id)
        .bind(&row.execution_status)
        .bind(&row.version)
        .bind(&row.standard)
        .bind(&row.event)
        .bind(&row.related_receipt_id)
        .bind(&row.related_receipt_receiver_id)
        .bind(&row.related_receipt_predecessor_id)
        .bind(&row.memo)
        .bind(&row.old_owner_id)
        .bind(&row.new_owner_id)
        .bind(&row.token_id)
        .bind(amount)
        .bind(&row.intent_hash)
        .bind(&row.referral)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}
