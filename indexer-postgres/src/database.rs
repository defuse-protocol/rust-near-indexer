use anyhow::ensure;
use chrono::{DateTime, Datelike, Utc};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::collections::BTreeSet;

use indexer_primitives::SilverDip4TransferRow;

const SAVE_ATTEMPTS: usize = 10;

/// Convert a NEAR nanosecond-epoch timestamp to a chrono DateTime<Utc>.
fn nanos_to_datetime(block_timestamp: u64) -> anyhow::Result<DateTime<Utc>> {
    let secs: i64 = (block_timestamp / 1_000_000_000)
        .try_into()
        .map_err(|_| anyhow::anyhow!("block_timestamp overflows i64: {block_timestamp}"))?;
    let nanos = (block_timestamp % 1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| anyhow::anyhow!("invalid block_timestamp: {block_timestamp}"))
}

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

    // Manual retry loop with exponential backoff so we can await partition creation
    let mut delay = std::time::Duration::from_millis(250);
    let max_delay = std::time::Duration::from_secs(60);

    for attempt in 1..=SAVE_ATTEMPTS {
        match try_insert_rows(pool, rows).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                indexer_common::metrics::DATABASE_INSERT_RETRIES_TOTAL.inc();

                // If the error is a missing partition, create it before retrying
                if is_missing_partition_error(&err) {
                    tracing::warn!(
                        target: indexer_common::config::INDEXER,
                        "Missing partition detected (attempt {}/{}), creating partitions...",
                        attempt,
                        SAVE_ATTEMPTS,
                    );
                    if let Err(partition_err) = create_partitions_for_rows(pool, rows).await {
                        tracing::error!(
                            target: indexer_common::config::INDEXER,
                            "Failed to create partitions: {}",
                            partition_err,
                        );
                    }
                } else {
                    tracing::warn!(
                        target: indexer_common::config::INDEXER,
                        "Failed to insert rows into PostgreSQL (attempt {}/{}): {}",
                        attempt,
                        SAVE_ATTEMPTS,
                        err,
                    );
                }

                if attempt == SAVE_ATTEMPTS {
                    return Err(err);
                }

                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(max_delay);
            }
        }
    }

    unreachable!()
}

/// Check if an error is caused by a missing partition in a partitioned table.
fn is_missing_partition_error(err: &anyhow::Error) -> bool {
    if let Some(db_err) = err.downcast_ref::<sqlx::Error>()
        && let sqlx::Error::Database(e) = db_err
    {
        return e.message().contains("no partition of relation");
    }
    false
}

/// Create monthly partitions for all distinct months present in the given rows.
/// Uses `CREATE TABLE IF NOT EXISTS` so it's idempotent.
async fn create_partitions_for_rows(
    pool: &PgPool,
    rows: &[SilverDip4TransferRow],
) -> anyhow::Result<()> {
    let months: BTreeSet<(i32, u32)> = rows
        .iter()
        .map(|row| {
            let dt = nanos_to_datetime(row.block_timestamp)?;
            Ok((dt.year(), dt.month()))
        })
        .collect::<anyhow::Result<_>>()?;

    for (year, month) in &months {
        // Validate year/month before interpolating into DDL (not parameterizable).
        // chrono guarantees month is 1..=12; we additionally bound the year to a
        // reasonable NEAR-era range to prevent nonsensical partition names.
        ensure!(
            *year >= 2020 && *year <= 2100,
            "Refusing to create partition for out-of-range year {year}"
        );

        let (next_year, next_month) = if *month == 12 {
            (year + 1, 1u32)
        } else {
            (*year, month + 1)
        };

        let partition_name = format!("silver_dip4_transfers_{:04}_{:02}", year, month);
        let from_date = format!("{:04}-{:02}-01", year, month);
        let to_date = format!("{:04}-{:02}-01", next_year, next_month);

        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {} PARTITION OF silver_dip4_transfers FOR VALUES FROM ('{}') TO ('{}')",
            partition_name, from_date, to_date
        );

        tracing::info!(
            target: indexer_common::config::INDEXER,
            "Creating partition: {}",
            partition_name,
        );

        sqlx::query(&ddl).execute(pool).await?;
    }

    Ok(())
}

async fn try_insert_rows(pool: &PgPool, rows: &[SilverDip4TransferRow]) -> anyhow::Result<()> {
    tracing::debug!(
        target: indexer_common::config::INDEXER,
        "Inserting {} rows into silver_dip4_transfers",
        rows.len()
    );

    let mut tx = pool.begin().await?;

    for row in rows {
        let amount: Option<sqlx::types::BigDecimal> =
            row.amount.parse::<sqlx::types::BigDecimal>().ok();

        let block_ts: DateTime<Utc> = nanos_to_datetime(row.block_timestamp)?;

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
