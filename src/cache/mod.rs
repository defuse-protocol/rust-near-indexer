use std::sync::Arc;
pub mod redis;

pub type ReceiptsCache = redis::RedisReceiptCache;
pub type ReceiptsCacheArc = Arc<ReceiptsCache>;

pub async fn init_cache(app_config: &crate::config::AppConfig) -> anyhow::Result<ReceiptsCacheArc> {
    let redis_url = app_config
        .redis_url
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("REDIS_URL must be set (local cache deprecated)"))?;
    let cache = redis::RedisReceiptCache::new(redis_url, app_config.redis_ttl_seconds).await?;
    tracing::info!("Initialized Redis receipt cache (redis-only mode)");
    Ok(Arc::new(cache))
}
