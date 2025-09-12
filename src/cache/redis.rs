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

    pub async fn get(
        &self,
        key: &ReceiptOrDataId,
    ) -> anyhow::Result<Option<ParentTransactionHashString>> {
        let redis_key = Self::key_receipt(key);
        let start = std::time::Instant::now();
        let mut conn = self.manager.clone();
        let res = conn
            .get::<_, Option<ParentTransactionHashString>>(&redis_key)
            .await;
        match res {
            Ok(v) => {
                tracing::trace!(op="get", key=%redis_key, hit=v.is_some(), ms=%start.elapsed().as_millis());
                Ok(v)
            }
            Err(e) => {
                tracing::warn!(op="get", key=%redis_key, error=%e, "redis error");
                Err(e.into())
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
    ) -> anyhow::Result<Option<ParentTransactionHashString>> {
        let redis_key = Self::key_potential(key);
        let mut conn = self.manager.clone();
        let res = conn
            .get::<_, Option<ParentTransactionHashString>>(&redis_key)
            .await;
        match res {
            Ok(v) => Ok(v),
            Err(e) => {
                tracing::warn!(op="potential_get", key=%redis_key, error=%e);
                Err(e.into())
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
    // Batches multiple SETEX calls into a single pipeline.
    pub async fn set_many_receipts(
        &self,
        keys: Vec<ReceiptOrDataId>,
        value: &ParentTransactionHashString,
    ) {
        if keys.is_empty() {
            return;
        }
        let count = keys.len();
        let mut conn = self.manager.clone();
        let mut pipe = redis::pipe();
        for k in keys {
            let redis_key = Self::key_receipt(&k);
            pipe.cmd("SETEX")
                .arg(&redis_key)
                .arg(self.ttl_seconds)
                .arg(value);
        }
        // Explicit type annotation for pipeline result
        let res: redis::RedisResult<()> = pipe.query_async(&mut conn).await;
        if let Err(e) = res {
            tracing::warn!(op="set_many", error=%e, "redis pipeline failed");
        } else {
            tracing::trace!(op="set_many", count=%count, "batched receipt mappings written");
        }
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        let mut conn = self.manager.clone();
        let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
        if pong.eq_ignore_ascii_case("PONG") {
            Ok(())
        } else {
            anyhow::bail!("Unexpected PING response: {pong}")
        }
    }
}
