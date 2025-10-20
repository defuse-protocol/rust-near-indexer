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
                tracing::trace!(
                    target: crate::config::INDEXER,
                    op="get",
                    key=%redis_key,
                    hit=v.is_some(),
                    ms=%start.elapsed().as_millis()
                );
                Ok(v)
            }
            Err(err) => {
                tracing::warn!(
                    target: crate::config::INDEXER,
                    op="get",
                    key=%redis_key,
                    error=%err,
                    "redis error"
                );
                Err(err.into())
            }
        }
    }

    pub async fn set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let redis_key = Self::key_receipt(&key);
        let start = std::time::Instant::now();
        let mut conn = self.manager.clone();
        if let Err(err) = conn
            .set_ex::<_, _, ()>(redis_key.clone(), value, self.ttl_seconds)
            .await
        {
            tracing::warn!(
                target: crate::config::INDEXER,
                op="set",
                key=%redis_key,
                error=%err,
                "redis set failed"
            );
        } else {
            tracing::trace!(
                target: crate::config::INDEXER,
                op="set",
                key=%redis_key,
                ms=%start.elapsed().as_millis()
            );
        }
    }

    pub async fn potential_get(
        &self,
        key: &ReceiptOrDataId,
    ) -> anyhow::Result<Option<ParentTransactionHashString>> {
        let redis_key = Self::key_potential(key);
        let start = std::time::Instant::now();
        let mut conn = self.manager.clone();
        let res = conn
            .get::<_, Option<ParentTransactionHashString>>(&redis_key)
            .await;
        match res {
            Ok(v) => {
                tracing::trace!(
                    target: crate::config::INDEXER,
                    op="potential_get",
                    key=%redis_key,
                    hit=v.is_some(),
                    ms=%start.elapsed().as_millis()
                );
                Ok(v)
            }
            Err(err) => {
                tracing::warn!(
                    target: crate::config::INDEXER,
                    op="potential_get",
                    key=%redis_key,
                    error=%err
                );
                Err(err.into())
            }
        }
    }

    pub async fn potential_set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let redis_key = Self::key_potential(&key);
        let mut conn = self.manager.clone();
        if let Err(err) = conn
            .set_ex::<_, _, ()>(redis_key.clone(), value, self.ttl_seconds)
            .await
        {
            tracing::warn!(
                target: crate::config::INDEXER,
                op="potential_set",
                key=%redis_key,
                error=%err
            );
        }
    }
    // Batches multiple SETEX calls into a single pipeline.
    pub async fn set_many_receipts(
        &self,
        keys: Vec<ReceiptOrDataId>,
        value: &ParentTransactionHashString,
    ) {
        self.internal_set_many_receipts(keys, value, Self::key_receipt)
            .await;
    }

    // Batches multiple SETEX calls into a single pipeline (potential)
    pub async fn set_many_potentials(
        &self,
        keys: Vec<ReceiptOrDataId>,
        value: &ParentTransactionHashString,
    ) {
        self.internal_set_many_receipts(keys, value, Self::key_potential)
            .await;
    }

    async fn internal_set_many_receipts(
        &self,
        keys: Vec<ReceiptOrDataId>,
        value: &ParentTransactionHashString,
        key_fn: fn(&ReceiptOrDataId) -> String,
    ) {
        if keys.is_empty() {
            return;
        }
        let count = keys.len();
        let mut conn = self.manager.clone();
        let mut pipe = redis::pipe();
        for k in keys {
            let redis_key = key_fn(&k);
            // Use modern SET with EX instead of legacy SETEX
            pipe.cmd("SET")
                .arg(&redis_key)
                .arg(value)
                .arg("EX")
                .arg(self.ttl_seconds);
        }
        // Explicit type annotation for pipeline result
        let res: redis::RedisResult<()> = pipe.query_async(&mut conn).await;
        if let Err(err) = res {
            tracing::warn!(
                target: crate::config::INDEXER,
                op="set_many",
                error=%err,
                "redis pipeline failed"
            );
        } else {
            tracing::trace!(
                target: crate::config::INDEXER,
                op="set_many",
                count=%count,
                "batched receipt mappings written"
            );
        }
    }
}
