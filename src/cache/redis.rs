use redis::AsyncCommands;

use crate::types::{ParentTransactionHashString, ReceiptOrDataId};

/// Redis-backed cache implementation.
pub struct RedisCache {
    manager: redis::aio::ConnectionManager,
    ttl_seconds: u64,
}

impl RedisCache {
    /// Create a new RedisCache instance.
    ///
    /// # Arguments
    /// * `url` - Redis server URL
    /// * `ttl_seconds` - TTL for cached values in seconds
    pub async fn new(url: &str, ttl_seconds: u64) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        Ok(Self {
            manager: client.get_connection_manager().await?,
            ttl_seconds,
        })
    }

    fn key_for_receipt(id: &ReceiptOrDataId) -> String {
        format!("receipt_cache:{}", Self::id_str(id))
    }

    fn key_for_potential(id: &ReceiptOrDataId) -> String {
        format!("potential_cache:{}", Self::id_str(id))
    }

    fn id_str(id: &ReceiptOrDataId) -> String {
        match id {
            ReceiptOrDataId::ReceiptId(h) => h.to_string(),
            ReceiptOrDataId::_DataId(h) => h.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl super::TxCache for RedisCache {
    async fn get(&mut self, key: &ReceiptOrDataId) -> Option<ParentTransactionHashString> {
        let conn = &mut self.manager;
        let redis_key = Self::key_for_receipt(key);
        conn.get(redis_key).await.unwrap_or_default()
    }

    async fn set(&mut self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let conn = &mut self.manager;
        let redis_key = Self::key_for_receipt(&key);
        let _ = conn
            .set_ex::<_, _, ()>(redis_key, value, self.ttl_seconds)
            .await;
    }

    async fn potential_get(
        &mut self,
        key: &ReceiptOrDataId,
    ) -> Option<ParentTransactionHashString> {
        let conn = &mut self.manager;
        let redis_key = Self::key_for_potential(key);
        conn.get(redis_key).await.unwrap_or_default()
    }

    async fn potential_set(&mut self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let conn = &mut self.manager;
        let redis_key = Self::key_for_potential(&key);
        let _ = conn
            .set_ex::<_, _, ()>(redis_key, value, self.ttl_seconds)
            .await;
    }
}
