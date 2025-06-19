use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use cached::{Cached, SizedCache};
use near_lake_framework::near_indexer_primitives::CryptoHash;
use std::sync::Arc;
use tokio::sync::Mutex;
use redis::{Client as RedisClient, RedisError, RedisResult, AsyncCommands};
use std::str::FromStr;
use std::collections::HashMap;

#[derive(Row, Serialize)]
pub struct EventRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub block_hash: String,
    pub contract_id: String,
    pub execution_status: String,
    pub version: String,
    pub standard: String,
    pub index_in_log: u64,
    pub event: String,
    pub data: String,
    pub related_receipt_id: String,
    pub related_receipt_receiver_id: String,
    pub related_receipt_predecessor_id: String,
    pub tx_hash: Option<String>,
}

#[derive(Deserialize)]
pub struct EventJson {
    pub version: String,
    pub standard: String,
    pub event: String,
    pub data: Value,
}

pub type ParentTransactionHashString = String;

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum ReceiptOrDataId {
    ReceiptId(CryptoHash),
}

pub struct ReceiptsCache {
    cache: SizedCache<ReceiptOrDataId, ParentTransactionHashString>,
    tx_hash_traces: HashMap<String, String>, // In-memory transaction hash traces
    redis_client: Option<RedisClient>,
    current_block_height: u64,
    expiration_blocks: u64,
}

impl ReceiptsCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: SizedCache::with_size(capacity),
            tx_hash_traces: HashMap::new(),
            redis_client: None,
            current_block_height: 0,
            expiration_blocks: 50,
        }
    }

    pub fn with_redis(capacity: usize, redis_client: RedisClient, expiration_blocks: u64) -> Result<Self, RedisError> {
        Ok(Self {
            cache: SizedCache::with_size(capacity),
            tx_hash_traces: HashMap::new(),
            redis_client: Some(redis_client),
            current_block_height: 0,
            expiration_blocks,
        })
    }

    pub fn cache_get(&mut self, key: &ReceiptOrDataId) -> Option<&ParentTransactionHashString> {
        self.cache.cache_get(key)
    }

    pub fn cache_set(&mut self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        self.cache.cache_set(key.clone(), value.clone());
        
        // Store Redis updates in a task to avoid blocking
        if let Some(ref client) = self.redis_client {
            let key_str = format!("receipt_id:{}", self.receipt_key_to_string(&key));
            let value_clone = value.clone();
            let block_height = self.current_block_height;
            let client_clone = client.clone();
            
            // Spawn a task to update Redis in the background
            tokio::spawn(async move {
                if let Ok(mut conn) = client_clone.get_multiplexed_async_connection().await {
                    let _: RedisResult<()> = redis::cmd("HSET")
                        .arg(&key_str)
                        .arg("tx_hash").arg(&value_clone)
                        .arg("block_height").arg(block_height.to_string())
                        .query_async(&mut conn)
                        .await;
                }
            });
        }
    }
    
    fn receipt_key_to_string(&self, key: &ReceiptOrDataId) -> String {
        match key {
            ReceiptOrDataId::ReceiptId(crypto_hash) => crypto_hash.to_string(),
        }
    }
    
    pub fn update_block_height(&mut self, height: u64) {
        self.current_block_height = height;
        
        let min_block_height = height.saturating_sub(self.expiration_blocks);
        self.tx_hash_traces.retain(|key, _| {
            if let Some(last_colon) = key.rfind(':') {
                if let Ok(key_block_height) = key[last_colon + 1..].parse::<u64>() {
                    return key_block_height >= min_block_height;
                }
            }
            false
        });
        
        if let Some(ref client) = self.redis_client {
            let client_clone = client.clone();
            let expiration_blocks = self.expiration_blocks;
            let current_height = self.current_block_height;
            
            tokio::spawn(async move {
                if let Ok(mut conn) = client_clone.get_multiplexed_async_connection().await {
                    let min_block_height = current_height.saturating_sub(expiration_blocks);
                    
                    if let Ok(keys) = conn.keys::<_, Vec<String>>("receipt_id:*").await {
                        for key in keys {
                            if let Ok(height_str) = conn.hget::<_, _, Option<String>>(&key, "block_height").await {
                                if let Some(height_str) = height_str {
                                    if let Ok(height) = height_str.parse::<u64>() {
                                        if height < min_block_height {
                                            let _: RedisResult<()> = conn.del(&key).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    if let Ok(keys) = conn.keys::<_, Vec<String>>("tx_trace:*").await {
                        for key in keys {
                            if let Some(last_colon) = key.rfind(':') {
                                if let Ok(key_block_height) = key[last_colon + 1..].parse::<u64>() {
                                    if key_block_height < min_block_height {
                                        let _: RedisResult<()> = conn.del(&key).await;
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
    }
    
    pub async fn load_from_redis(&mut self) -> Result<(), RedisError> {
        if let Some(ref client) = self.redis_client {
            let mut conn = client.get_multiplexed_async_connection().await?;
            
            let min_block_height = if self.current_block_height > self.expiration_blocks {
                self.current_block_height - self.expiration_blocks
            } else {
                0
            };
            
            let keys: Vec<String> = conn.keys("receipt_id:*").await?;
            
            for key in keys {
                let block_height: Option<String> = conn.hget(&key, "block_height").await?;
                
                if let Some(height_str) = block_height {
                    if let Ok(height) = height_str.parse::<u64>() {
                        if height >= min_block_height {
                            let tx_hash: Option<String> = conn.hget(&key, "tx_hash").await?;
                            if let Some(tx_hash_value) = tx_hash {
                                let key_str = key.strip_prefix("receipt_id:").unwrap_or(&key);
                                if let Ok(crypto_hash) = CryptoHash::from_str(key_str) {
                                    let receipt_id = ReceiptOrDataId::ReceiptId(crypto_hash);
                                    self.cache.cache_set(receipt_id, tx_hash_value);
                                }
                            }
                        } else {
                            let _: RedisResult<()> = conn.del(&key).await;
                        }
                    }
                }
            }
            
            let trace_keys: Vec<String> = conn.keys("tx_trace:*").await?;
            
            for key in trace_keys {
                if let Some(last_colon) = key.rfind(':') {
                    if let Ok(key_block_height) = key[last_colon + 1..].parse::<u64>() {
                        if key_block_height >= min_block_height {
                            if let Ok(tx_hash) = conn.get::<_, String>(&key).await {
                                self.tx_hash_traces.insert(key.clone(), tx_hash);
                            }
                        } else {
                            let _: RedisResult<()> = conn.del(&key).await;
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    pub async fn trace_tx_hash(
        &mut self,
        signer_id: &str,
        signer_public_key: &str,
        current_block_height: u64,
    ) -> Option<String> {
        let signer_key = format!("{}:{}", signer_id, signer_public_key);
        
        let start_block = current_block_height.saturating_sub(10);
        
        for block_height in (start_block..current_block_height).rev() {
            let trace_key = format!("tx_trace:{}:{}", signer_key, block_height);
            
            if let Some(tx_hash) = self.tx_hash_traces.get(&trace_key) {
                return Some(tx_hash.clone());
            }
            
            if let Some(ref client) = self.redis_client {
                if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                    if let Ok(tx_hash) = conn.get::<_, String>(&trace_key).await {
                        self.tx_hash_traces.insert(trace_key, tx_hash.clone());
                        return Some(tx_hash);
                    }
                }
            }
        }
        
        None
    }

    pub async fn cache_tx_hash_trace(
        &mut self,
        signer_id: &str,
        signer_public_key: &str,
        block_height: u64,
        tx_hash: String,
    ) {
        let trace_key = format!("tx_trace:{}:{}:{}", signer_id, signer_public_key, block_height);
        
        self.tx_hash_traces.insert(trace_key.clone(), tx_hash.clone());
        
        if let Some(ref client) = self.redis_client {
            if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                let _: redis::RedisResult<()> = conn.set_ex(
                    &trace_key,
                    &tx_hash,
                    self.expiration_blocks * 60,
                ).await;
            }
        }
    }
}

pub type ReceiptsCacheArc = Arc<Mutex<ReceiptsCache>>;
