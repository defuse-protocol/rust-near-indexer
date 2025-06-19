use serde::Deserialize;
use std::env;
use tracing::info;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub indexer: IndexerConfig,
    pub checkpoint: CheckpointConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexerConfig {
    pub enabled: bool,
    pub block_height: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CheckpointConfig {
    pub enabled: bool,
    pub interval_hours: u64,
    pub chunk_size: u64,
    pub max_retries: u32,
    pub retry_delay_ms: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            indexer: IndexerConfig {
                enabled: true,
                block_height: 0,
            },
            checkpoint: CheckpointConfig {
                enabled: true,
                interval_hours: 24,
                chunk_size: 100000,
                max_retries: 10,
                retry_delay_ms: 5000,
            },
        }
    }
}

impl AppConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(enabled) = env::var("INDEXER_ENABLED") {
            config.indexer.enabled = enabled.to_lowercase() == "true";
        }
        if let Ok(block_height) = env::var("BLOCK_HEIGHT") {
            if let Ok(height) = block_height.parse() {
                config.indexer.block_height = height;
            }
        }

        if let Ok(enabled) = env::var("CHECKPOINT_ENABLED") {
            config.checkpoint.enabled = enabled.to_lowercase() == "true";
        }
        if let Ok(interval) = env::var("CHECKPOINT_INTERVAL_HOURS") {
            if let Ok(hours) = interval.parse() {
                config.checkpoint.interval_hours = hours;
            }
        }
        if let Ok(chunk_size) = env::var("CHECKPOINT_CHUNK_SIZE") {
            if let Ok(size) = chunk_size.parse() {
                config.checkpoint.chunk_size = size;
            }
        }
        if let Ok(max_retries) = env::var("CHECKPOINT_MAX_RETRIES") {
            if let Ok(retries) = max_retries.parse() {
                config.checkpoint.max_retries = retries;
            }
        }
        if let Ok(retry_delay) = env::var("CHECKPOINT_RETRY_DELAY_MS") {
            if let Ok(delay) = retry_delay.parse() {
                config.checkpoint.retry_delay_ms = delay;
            }
        }

        config
    }

    pub fn log_config(&self) {
        info!("Application configuration:");
        info!("Indexer: enabled={}", self.indexer.enabled);
        info!("Checkpoint: enabled={}, interval={}h, chunk_size={}, max_retries={}, retry_delay={}ms",
            self.checkpoint.enabled,
            self.checkpoint.interval_hours,
            self.checkpoint.chunk_size,
            self.checkpoint.max_retries,
            self.checkpoint.retry_delay_ms
        );
    }
}

pub fn init_tracing() {
    let env_filter = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .init();
}
