use std::sync::Arc;
pub mod redis;

pub type ReceiptsCache = redis::RedisReceiptCache;
pub type ReceiptsCacheArc = Arc<ReceiptsCache>;

pub async fn init_cache(
    redis_url: &str,
    redis_ttl_seconds: u64,
) -> anyhow::Result<ReceiptsCacheArc> {
    let cache = redis::RedisReceiptCache::new(redis_url, redis_ttl_seconds).await?;
    tracing::info!(
        target: crate::config::INDEXER,
        "Initialized Redis receipt cache (redis-only mode, ping ok)"
    );
    Ok(Arc::new(cache))
}
