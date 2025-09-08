use redis::Commands;
use tokio::task;

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

    /// Run a blocking closure in a way that doesn't block the Tokio runtime worker threads.
    /// If called within a Tokio runtime, `block_in_place` is used to offload to the blocking pool.
    fn run_blocking<F, R>(f: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        // If we're inside a Tokio runtime, boost to the blocking pool to avoid starving the runtime.
        if tokio::runtime::Handle::try_current().is_ok() {
            task::block_in_place(f)
        } else {
            // Not inside a Tokio runtime (e.g., during startup tests) — run directly.
            f()
        }
    }
}

impl super::TxCache for RedisCache {
    fn get(&self, key: &ReceiptOrDataId) -> Option<ParentTransactionHashString> {
        Self::run_blocking(|| {
            let mut conn = match self.client.get_connection() {
                Ok(c) => c,
                Err(_) => return None,
            };
            let redis_key = Self::key_for_receipt(key);
            match conn.get(redis_key) {
                Ok(val) => val,
                Err(_) => None,
            }
        })
    }

    fn set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        Self::run_blocking(|| {
            let mut conn = match self.client.get_connection() {
                Ok(c) => c,
                Err(_) => return,
            };
            let redis_key = Self::key_for_receipt(&key);
            let _ = conn.set_ex::<_, _, ()>(redis_key, value, self.ttl_seconds);
        })
    }

    fn potential_get(&self, key: &ReceiptOrDataId) -> Option<ParentTransactionHashString> {
        Self::run_blocking(|| {
            let mut conn = match self.client.get_connection() {
                Ok(c) => c,
                Err(_) => return None,
            };
            let redis_key = format!("potential_cache:{:?}", key);
            match conn.get(redis_key) {
                Ok(val) => val,
                Err(_) => None,
            }
        })
    }

    fn potential_set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        Self::run_blocking(|| {
            let mut conn = match self.client.get_connection() {
                Ok(c) => c,
                Err(_) => return,
            };
            let redis_key = format!("potential_cache:{:?}", key);
            let _ = conn.set_ex::<_, _, ()>(redis_key, value, self.ttl_seconds);
        })
    }
}
