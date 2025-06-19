use anyhow::{Result, anyhow};
use clickhouse::Client;
use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::config::CheckpointConfig;
use chrono::{DateTime, Utc, TimeZone};

const NANOS_IN_SECOND: u64 = 1_000_000_000;

fn nanos_to_datetime(nanos: u64) -> Result<DateTime<Utc>> {
    let secs = (nanos / NANOS_IN_SECOND) as i64;
    let nsecs = (nanos % NANOS_IN_SECOND) as u32;
    Utc.timestamp_opt(secs, nsecs)
        .single()
        .ok_or_else(|| anyhow!("Invalid timestamp: {} nanoseconds", nanos))
}

fn datetime_to_nanos(dt: DateTime<Utc>) -> u64 {
    (dt.timestamp() as u64) * NANOS_IN_SECOND + dt.timestamp_subsec_nanos() as u64
}

pub struct TokenHoldersCheckpoint {
    client: Client,
    is_processing: Arc<Mutex<bool>>,
    config: CheckpointConfig,
}

impl TokenHoldersCheckpoint {
    pub fn new(client: Client, config: CheckpointConfig) -> Self {
        Self { 
            client,
            is_processing: Arc::new(Mutex::new(false)),
            config,
        }
    }

    pub async fn start(&self) -> Result<()> {
        if !self.config.enabled {
            info!("Token holders checkpoint is disabled");
            return Ok(());
        }

        let mut interval = time::interval(Duration::from_secs(self.config.interval_hours * 3600));

        loop {
            interval.tick().await;
            
            let mut is_processing = self.is_processing.lock().await;
            if *is_processing {
                warn!("Previous checkpoint job is still running, skipping this iteration");
                continue;
            }
            *is_processing = true;
            drop(is_processing);

            if let Err(e) = self.process_checkpoint().await {
                error!("Failed to process token holders checkpoint: {}", e);
            }

            let mut is_processing = self.is_processing.lock().await;
            *is_processing = false;
        }
    }

    async fn process_checkpoint(&self) -> Result<()> {
        info!("Starting token holders checkpoint processing");
        
        let latest_checkpoint_timestamp = self.get_latest_checkpoint_timestamp().await?;
        info!("Latest checkpoint timestamp: {:?}", latest_checkpoint_timestamp);

        let latest_transfer_timestamp = self.get_latest_245_event_timestamp().await?;
        info!("Latest transfer timestamp: {:?}", latest_transfer_timestamp);

        if let Some(latest_transfer) = latest_transfer_timestamp {
            let mut current_block = match latest_checkpoint_timestamp {
                Some(checkpoint) => {
                    if checkpoint >= latest_transfer {
                        error!("Corrupted state detected. No new data to process: {:?} (checkpoint) >= {:?} (latest_transfer)", checkpoint, latest_transfer);
                        return Err(anyhow::anyhow!("Corrupted state detected. No new data to process"));
                    }
                    
                    self.get_block_height_for_timestamp(checkpoint).await?
                }
                None => {
                    let earliest_event = self.get_earliest_245_event_timestamp().await?;
                    info!("No checkpoint found, starting from earliest transfer timestamp: {:?}", earliest_event);
                    let earliest_height = self.get_block_height_for_timestamp(earliest_event).await?;
                    // because we want this event to be processed
                    earliest_height - 1
                }
            };

            let end_block = self.get_block_height_for_timestamp(latest_transfer).await?;

            info!("From {:?} to {:?}", current_block, end_block);
            while current_block < end_block {
                let chunk_end = std::cmp::min(current_block + self.config.chunk_size, end_block);
                info!("Processing blocks from {} to {}", current_block, chunk_end);

                let mut retry_count = 0;
                while retry_count < self.config.max_retries {
                    match self.process_chunk(current_block, chunk_end).await {
                        Ok(_) => {
                            info!("Successfully processed chunk {} to {}", current_block, chunk_end);
                            break;
                        }
                        Err(e) => {
                            retry_count += 1;
                            if retry_count == self.config.max_retries {
                                error!("Failed to process chunk after {} retries: {}", self.config.max_retries, e);
                                return Err(e);
                            }
                            warn!("Retry {} for chunk {} to {}: {}", retry_count, current_block, chunk_end, e);
                            time::sleep(Duration::from_millis(self.config.retry_delay_ms)).await;
                        }
                    }
                }
                current_block = chunk_end;
            }
        } else {
            info!("No transfer events found in the database");
        }

        info!("Token holders checkpoint processing completed");
        Ok(())
    }

    async fn get_latest_checkpoint_timestamp(&self) -> Result<Option<DateTime<Utc>>> {
        let timestamp: Option<u64> = self.client
            .query("SELECT MAX(block_timestamp) FROM token_holders_checkpoints")
            .fetch_optional()
            .await?;
        
        match timestamp {
            Some(0) | None => Ok(None),
            Some(ts) => Ok(Some(nanos_to_datetime(ts)?))
        }
    }

    async fn get_latest_245_event_timestamp(&self) -> Result<Option<DateTime<Utc>>> {
        let timestamp: Option<u64> = self.client
            .query("SELECT MAX(block_timestamp) FROM silver_nep_245_events")
            .fetch_optional()
            .await?;
        
        match timestamp {
            Some(0) | None => Ok(None),
            Some(ts) => Ok(Some(nanos_to_datetime(ts)?))
        }
    }

    async fn get_earliest_245_event_timestamp(&self) -> Result<DateTime<Utc>> {
        let timestamp: Option<u64> = self.client
            .query("SELECT MIN(block_timestamp) FROM silver_nep_245_events")
            .fetch_optional()
            .await?;
        
        match timestamp {
            Some(0) | None => Err(anyhow!("No transfer events found in the database")),
            Some(ts) => nanos_to_datetime(ts)
        }
    }

    async fn get_block_height_for_timestamp(&self, timestamp: DateTime<Utc>) -> Result<u64> {
        let nanos = datetime_to_nanos(timestamp);
        
        let block_height: Option<u64> = self.client
            .query("
                SELECT block_height 
                FROM silver_nep_245_events 
                ORDER BY abs(toUnixTimestamp64Nano(block_timestamp) - ?) ASC  
                LIMIT 1
            ")
            .bind(nanos)
            .fetch_optional()
            .await?;
        
        block_height.ok_or_else(|| anyhow!("No blocks found near timestamp: {} (nanos: {})", timestamp, nanos))
    }

    async fn process_chunk(&self, start_block: u64, end_block: u64) -> Result<()> {
        let query = r#"
            WITH 
            transfers AS (
                SELECT 
                    block_height,
                    block_timestamp,
                    block_hash,
                    old_owner_id as account_id,
                    token_id as asset_id,
                    -amount as balance
                FROM silver_nep_245_events
                WHERE event = 'mt_transfer'
                AND block_height >= ?
                AND block_height <= ?
                AND old_owner_id IS NOT NULL
                AND amount IS NOT NULL
                
                UNION ALL
                
                SELECT 
                    block_height,
                    block_timestamp,
                    block_hash,
                    new_owner_id as account_id,
                    token_id as asset_id,
                    amount as balance
                FROM silver_nep_245_events
                WHERE event = 'mt_transfer'
                AND block_height >= ?
                AND block_height <= ?
                AND new_owner_id IS NOT NULL
                AND amount IS NOT NULL
                
                UNION ALL
                
                SELECT 
                    block_height,
                    block_timestamp,
                    block_hash,
                    new_owner_id as account_id,
                    token_id as asset_id,
                    amount as balance
                FROM silver_nep_245_events
                WHERE event = 'mt_mint'
                AND block_height >= ?
                AND block_height <= ?
                AND new_owner_id IS NOT NULL
                AND amount IS NOT NULL
                
                UNION ALL
                
                SELECT 
                    block_height,
                    block_timestamp,
                    block_hash,
                    old_owner_id as account_id,
                    token_id as asset_id,
                    -amount as balance
                FROM silver_nep_245_events
                WHERE event = 'mt_burn'
                AND block_height >= ?
                AND block_height <= ?
                AND old_owner_id IS NOT NULL
                AND amount IS NOT NULL
            )
            INSERT INTO token_holders_checkpoints
            SELECT 
                block_height,
                max(block_timestamp) as block_timestamp,
                any(block_hash) as block_hash,
                account_id,
                asset_id,
                sum(balance) as balance
            FROM transfers
            GROUP BY block_height, account_id, asset_id
            ORDER BY block_height, account_id, asset_id
        "#;

        self.client
            .query(query)
            .bind(start_block)
            .bind(end_block)
            .bind(start_block)
            .bind(end_block)
            .bind(start_block)
            .bind(end_block)
            .bind(start_block)
            .bind(end_block)
            .execute()
            .await?;

        Ok(())
    }
} 
