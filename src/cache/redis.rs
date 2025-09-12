use crate::types::{ParentTransactionHashString, ReceiptOrDataId};
use redis::AsyncCommands;

#[derive(Clone)]
pub struct RedisReceiptCache {
    manager: redis::aio::ConnectionManager,
    ttl_seconds: u64,
}

impl RedisReceiptCache {
    pub async fn new(url: &str, ttl_seconds: u64) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        Ok(Self {
            manager: client.get_connection_manager().await?,
            ttl_seconds,
        })
    }

    fn key_receipt(id: &ReceiptOrDataId) -> String {
        format!("receipt_cache:{}", Self::id_str(id))
    }
    fn key_potential(id: &ReceiptOrDataId) -> String {
        format!("potential_cache:{}", Self::id_str(id))
    }
    fn id_str(id: &ReceiptOrDataId) -> String {
        match id {
            ReceiptOrDataId::ReceiptId(h) => h.to_string(),
            ReceiptOrDataId::_DataId(h) => h.to_string(),
        }
    }

    pub async fn get(&self, key: &ReceiptOrDataId) -> Option<ParentTransactionHashString> {
        let redis_key = Self::key_receipt(key);
        let start = std::time::Instant::now();
        let mut conn = self.manager.clone();
        match conn
            .get::<_, Option<ParentTransactionHashString>>(&redis_key)
            .await
        {
            Ok(v) => {
                tracing::trace!(op="get", key=%redis_key, hit=v.is_some(), ms=%start.elapsed().as_millis());
                v
            }
            Err(e) => {
                tracing::warn!(op="get", key=%redis_key, error=%e, "redis error");
                None
            }
        }
    }

    pub async fn set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let redis_key = Self::key_receipt(&key);
        let start = std::time::Instant::now();
        let mut conn = self.manager.clone();
        if let Err(e) = conn
            .set_ex::<_, _, ()>(redis_key.clone(), value, self.ttl_seconds)
            .await
        {
            tracing::warn!(op="set", key=%redis_key, error=%e, "redis set failed");
        } else {
            tracing::trace!(op="set", key=%redis_key, ms=%start.elapsed().as_millis());
        }
    }

    pub async fn potential_get(
        &self,
        key: &ReceiptOrDataId,
    ) -> Option<ParentTransactionHashString> {
        let redis_key = Self::key_potential(key);
        let mut conn = self.manager.clone();
        match conn
            .get::<_, Option<ParentTransactionHashString>>(&redis_key)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(op="potential_get", key=%redis_key, error=%e);
                None
            }
        }
    }

    pub async fn potential_set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let redis_key = Self::key_potential(&key);
        let mut conn = self.manager.clone();
        if let Err(e) = conn
            .set_ex::<_, _, ()>(redis_key.clone(), value, self.ttl_seconds)
            .await
        {
            tracing::warn!(op="potential_set", key=%redis_key, error=%e);
        }
    }
}
