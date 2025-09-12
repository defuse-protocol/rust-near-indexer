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
        let span = tracing::debug_span!("redis_cache_get", key = Self::id_str(key));
        let _enter = span.enter();

        let conn = &mut self.manager;
        let redis_key = Self::key_for_receipt(key);
        let start = std::time::Instant::now();
        match conn
            .get::<_, Option<ParentTransactionHashString>>(&redis_key)
            .await
        {
            Ok(v) => {
                let duration = start.elapsed();
                tracing::debug!(
                    duration_ms = duration.as_millis(),
                    hit = v.is_some(),
                    "Redis GET completed"
                );
                v
            }
            Err(e) => {
                tracing::warn!("Redis GET failed for key={}: {}", redis_key, e);
                None
            }
        }
    }

    async fn set(&mut self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let span = tracing::debug_span!("redis_cache_set", key = Self::id_str(&key));
        let _enter = span.enter();

        let conn = &mut self.manager;
        let redis_key = Self::key_for_receipt(&key);
        let start = std::time::Instant::now();
        let result = conn
            .set_ex::<_, _, ()>(redis_key, value, self.ttl_seconds)
            .await;
        let duration = start.elapsed();
        tracing::debug!(
            duration_ms = duration.as_millis(),
            success = result.is_ok(),
            "Redis SET completed"
        );
    }

    async fn potential_get(
        &mut self,
        key: &ReceiptOrDataId,
    ) -> Option<ParentTransactionHashString> {
        let span = tracing::debug_span!("redis_potential_cache_get", key = Self::id_str(key));
        let _enter = span.enter();

        let conn = &mut self.manager;
        let redis_key = Self::key_for_potential(key);
        let start = std::time::Instant::now();
        match conn
            .get::<_, Option<ParentTransactionHashString>>(&redis_key)
            .await
        {
            Ok(v) => {
                let duration = start.elapsed();
                tracing::debug!(
                    duration_ms = duration.as_millis(),
                    hit = v.is_some(),
                    "Redis potential GET completed"
                );
                v
            }
            Err(e) => {
                tracing::warn!("Redis GET failed for key={}: {}", redis_key, e);
                None
            }
        }
    }

    async fn potential_set(&mut self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let span = tracing::debug_span!("redis_potential_cache_set", key = Self::id_str(&key));
        let _enter = span.enter();

        let conn = &mut self.manager;
        let redis_key = Self::key_for_potential(&key);
        let start = std::time::Instant::now();
        let result = conn
            .set_ex::<_, _, ()>(redis_key, value, self.ttl_seconds)
            .await;
        let duration = start.elapsed();
        tracing::debug!(
            duration_ms = duration.as_millis(),
            success = result.is_ok(),
            "Redis potential SET completed"
        );
    }
}
