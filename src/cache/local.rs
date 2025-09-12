use tokio::sync::RwLock;

use lru::LruCache;

use crate::types::{ParentTransactionHashString, ReceiptOrDataId};

pub struct LocalCache {
    // This is a main pipe for receipt-tx cache for the data we definitely want to store
    watchlist_cache: RwLock<LruCache<ReceiptOrDataId, ParentTransactionHashString>>,
    // This is a secondary pipe for receipt-tx cache for the data we might want to store
    // (e.g. we observed a receipt that involves any account of interest, but the parent tx
    // did not involve any account of interest, so we didn't store it initially)
    potential_cache: RwLock<LruCache<ReceiptOrDataId, ParentTransactionHashString>>,
}

impl LocalCache {
    pub fn new(size: usize) -> anyhow::Result<Self> {
        Ok(Self {
            watchlist_cache: RwLock::new(LruCache::new(std::num::NonZero::new(size).unwrap())),
            potential_cache: RwLock::new(LruCache::new(std::num::NonZeroUsize::new(size).unwrap())),
        })
    }
}

#[async_trait::async_trait]
impl super::TxCache for LocalCache {
    async fn get(&mut self, key: &ReceiptOrDataId) -> Option<ParentTransactionHashString> {
        let span = tracing::debug_span!("local_cache_get");
        let _enter = span.enter();

        let start = std::time::Instant::now();
        let mut cache = self.watchlist_cache.write().await;
        let result = cache.get(key).cloned();
        let duration = start.elapsed();
        tracing::debug!(
            duration_ms = duration.as_millis(),
            hit = result.is_some(),
            "Local cache GET completed"
        );
        result
    }

    async fn set(&mut self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let span = tracing::debug_span!("local_cache_set");
        let _enter = span.enter();

        let start = std::time::Instant::now();
        let mut cache = self.watchlist_cache.write().await;
        cache.put(key, value);
        let duration = start.elapsed();
        tracing::debug!(
            duration_ms = duration.as_millis(),
            "Local cache SET completed"
        );
    }

    async fn potential_get(
        &mut self,
        key: &ReceiptOrDataId,
    ) -> Option<ParentTransactionHashString> {
        let mut cache = self.potential_cache.write().await;
        cache.get(key).cloned()
    }

    async fn potential_set(&mut self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let mut cache = self.potential_cache.write().await;
        cache.put(key, value);
    }
}
