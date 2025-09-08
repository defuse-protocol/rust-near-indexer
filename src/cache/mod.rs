use std::sync::Arc;

use tokio::sync::Mutex;

use crate::types::{ParentTransactionHashString, ReceiptOrDataId};

pub mod local;
pub mod redis;

/// Trait for transaction/receipt cache abstraction.
///
/// The main cache stores hashes and data for transactions/receipts that are directly relevant to accounts of interest.
/// The potential cache stores hashes and data for transactions/receipts that may become relevant later, allowing deferred association if interest is proven.
/// This separation keeps the main cache clean and enables tracing indirect relationships when needed.
pub trait TxCache: Send + Sync {
    fn get(&self, key: &ReceiptOrDataId) -> Option<ParentTransactionHashString>;
    fn set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString);

    fn potential_get(&self, key: &ReceiptOrDataId) -> Option<ParentTransactionHashString>;
    fn potential_set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString);
}

pub type ReceiptsCacheArc = Arc<Mutex<Box<dyn TxCache + Send + Sync>>>;

pub fn init_cache(app_config: &crate::config::AppConfig) -> ReceiptsCacheArc {
    if let Some(redis_url) = &app_config.redis_url {
        let redis_cache = redis::RedisCache::new(redis_url, app_config.redis_ttl_seconds);
        tracing::info!("Initialized Redis cache");
        Arc::new(Mutex::new(Box::new(redis_cache)))
    } else {
        tracing::warn!("Initialized local in-memory cache");
        let local_cache = local::LocalCache::new(100_000);
        Arc::new(Mutex::new(Box::new(local_cache)))
    }
}
