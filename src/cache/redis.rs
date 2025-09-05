use redis::Commands;

use crate::types::{ParentTransactionHashString, ReceiptOrDataId};

/// Redis-backed cache implementation.
pub struct RedisCache {
    client: redis::Client,
    ttl_seconds: u64,
}

impl RedisCache {
    /// Create a new RedisCache instance.
    ///
    /// # Arguments
    /// * `url` - Redis server URL
    /// * `ttl_seconds` - TTL for cached values in seconds
    pub fn new(url: &str, ttl_seconds: u64) -> Self {
        let client = redis::Client::open(url).expect("Failed to create Redis client");
        Self {
            client,
            ttl_seconds,
        }
    }

    fn key_for_receipt(id: &ReceiptOrDataId) -> String {
        format!("receipt_cache:{:?}", id)
    }
}

impl super::TxCache for RedisCache {
    fn get(&self, key: &ReceiptOrDataId) -> Option<ParentTransactionHashString> {
        let mut conn = match self.client.get_connection() {
            Ok(c) => c,
            Err(_) => return None,
        };
        let redis_key = Self::key_for_receipt(key);
        match conn.get(redis_key) {
            Ok(val) => val,
            Err(_) => None,
        }
    }

    fn set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let mut conn = match self.client.get_connection() {
            Ok(c) => c,
            Err(_) => return,
        };
        let redis_key = Self::key_for_receipt(&key);
        let _ = conn.set_ex::<_, _, ()>(redis_key, value, self.ttl_seconds);
    }
}
