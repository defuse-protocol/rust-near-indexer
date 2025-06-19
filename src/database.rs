use clickhouse::{Client, Row};
use std::env;
use redis::{Client as RedisClient, RedisError};
use crate::retry::{with_retry, is_network_error};

// Initializes the Clickhouse client using environment variables.
/// Environment variables required:
/// - `CLICKHOUSE_URL`
/// - `CLICKHOUSE_USER`
/// - `CLICKHOUSE_PASSWORD`
/// - `CLICKHOUSE_DATABASE`
pub fn init_clickhouse_client() -> Client {
    let url = env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL not set in environment");
    let user = env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER not set in environment");
    let password = env::var("CLICKHOUSE_PASSWORD").expect("CLICKHOUSE_PASSWORD not set in environment");
    let database = env::var("CLICKHOUSE_DATABASE").expect("CLICKHOUSE_DATABASE not set in environment");

    Client::default()
        .with_url(&url)
        .with_user(&user)
        .with_password(&password)
        .with_database(&database)
        .with_option("connect_timeout", "10")
        .with_option("receive_timeout", "30")
        .with_option("send_timeout", "30")
}

/// Initializes Redis client using environment variables.
/// Environment variables required:
/// - `REDIS_URL` (optional, will return None if not set)
pub async fn init_redis_client() -> Option<RedisClient> {
    match env::var("REDIS_URL") {
        Ok(url) => {
            match RedisClient::open(url) {
                Ok(client) => {
                    // Check connection before returning
                    match check_redis_connection(&client).await { Err(e) => {
                        eprintln!("Warning: Redis connection check failed: {}", e);
                        None
                    } _ => {
                        println!("Redis connection verified successfully");
                        Some(client)
                    }}
                },
                Err(e) => {
                    eprintln!("Failed to create Redis client: {}", e);
                    None
                }
            }
        },
        Err(_) => {
            println!("No REDIS_URL provided, transaction cache will not be persisted");
            None
        }
    }
}

pub async fn get_last_height(client: &Client) -> Result<u64, clickhouse::error::Error> {
    client
        .query("SELECT max(block_height) FROM events")
        .fetch_one::<u64>()
        .await
}

pub async fn insert_rows(client: &Client, rows: &[impl Row + serde::Serialize]) -> Result<(), Box<dyn std::error::Error>> {
    let rows_ref = rows; // Create a reference we can move into the closure
    
    with_retry(
        || async { insert_rows_internal(client, rows_ref).await },
        5,
        |e| is_network_error(&e.to_string()),
        "database insert"
    ).await
}

async fn insert_rows_internal(client: &Client, rows: &[impl Row + serde::Serialize]) -> Result<(), Box<dyn std::error::Error>> {
    let mut insert = client.insert("events")?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

/// Checks if Redis is available and functioning properly
pub async fn check_redis_connection(client: &RedisClient) -> Result<(), RedisError> {
    let mut conn = client.get_multiplexed_async_connection().await?;
    
    let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
    
    if pong != "PONG" {
        return Err(RedisError::from(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Redis PING did not return PONG",
        )));
    }
    
    Ok(())
}

