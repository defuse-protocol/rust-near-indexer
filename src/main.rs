mod config;
mod database;
mod event_handler;
mod types;
mod retry;
mod token_holders_checkpoint;

use crate::config::{init_tracing, AppConfig};
use crate::database::{get_last_height, init_clickhouse_client, init_redis_client};
use crate::event_handler::handle_stream;
use crate::retry::with_retry;
use crate::token_holders_checkpoint::TokenHoldersCheckpoint;
use tracing::error;

use near_lake_framework::LakeConfigBuilder;

async fn check_clickhouse_connection(client: &clickhouse::Client) -> Result<(), Box<dyn std::error::Error>> {
    println!("Checking ClickHouse connection...");
    
    with_retry(
        || async { 
            match client.query("SELECT 1").fetch_one::<u8>().await {
                Ok(1) => Ok(()),
                Ok(value) => Err(format!("Unexpected response from ClickHouse: {} (expected 1)", value)),
                Err(err) => Err(err.to_string()),
            }
        },
        5,
        |_| true, // All errors are retriable for connection check
        "ClickHouse connection check"
    ).await?;
    
    println!("ClickHouse connection successful!");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    println!("Starting NEAR Defuse Indexer...");

    let config = AppConfig::from_env();
    config.log_config();

    println!("Initializing ClickHouse client...");
    let clickhouse_client = init_clickhouse_client();
    
    if let Err(err) = check_clickhouse_connection(&clickhouse_client).await {
        return Err(format!("ClickHouse health check failed: {}", err).into());
    }
    
    println!("Initializing Redis client...");
    let redis_client = init_redis_client().await;

    if config.checkpoint.enabled {
        let checkpoint_client = clickhouse_client.clone();
        let checkpoint_config = config.checkpoint.clone();
        tokio::spawn(async move {
            let checkpoint = TokenHoldersCheckpoint::new(checkpoint_client, checkpoint_config);
            if let Err(e) = checkpoint.start().await {
                error!("Token holders checkpoint job failed: {}", e);
            }
        });
    }

    if config.indexer.enabled {
        println!("Getting last processed block height...");
        let last_height = match get_last_height(&clickhouse_client).await {
            Ok(height) => height,
            Err(e) => {
                println!("Warning: Failed to get last height: {}. Starting from configured block height.", e);
                0
            }
        };
        
        let start_block = config.indexer.block_height.max(last_height + 1);
        
        println!("Starting indexer at block height: {}", start_block);

        let lake_config = match LakeConfigBuilder::default()
            .mainnet()
            .start_block_height(start_block)
            .build() {
                Ok(config) => config,
                Err(e) => return Err(format!("Failed to create NEAR Lake Framework config: {}", e).into()),
            };

        println!("Starting stream processing...");
        handle_stream(lake_config, clickhouse_client, redis_client).await;
    } else {
        println!("Indexer is disabled, waiting for checkpoint job...");
        // Keep the main thread alive
        tokio::signal::ctrl_c().await?;
    }

    Ok(())
}