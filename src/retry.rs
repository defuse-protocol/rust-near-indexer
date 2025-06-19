use std::future::Future;
use std::time::Duration;

/// Generic retry utility that handles transient errors with exponential backoff
/// 
/// # Parameters
/// - `operation`: The async operation to retry
/// - `max_retries`: Maximum number of retry attempts
/// - `is_retriable_error`: Function that determines if an error should be retried
/// - `operation_name`: Name of the operation for logging purposes
/// 
/// # Returns
/// - Result from the operation or error after exhausting retries
pub async fn with_retry<F, Fut, T, E>(
    operation: F,
    max_retries: usize,
    is_retriable_error: impl Fn(&E) -> bool,
    operation_name: &str,
) -> Result<T, Box<dyn std::error::Error>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display + 'static,
{
    let mut retry_count = 0;
    let mut last_error = None;

    while retry_count < max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                let error_msg = e.to_string();
                eprintln!("Error during {}: {}", operation_name, error_msg);
                
                if is_retriable_error(&e) {
                    retry_count += 1;
                    
                    if retry_count < max_retries {
                        let backoff = get_backoff_duration(retry_count);
                        println!(
                            "Retrying {} ({}/{}) in {}ms...", 
                            operation_name, retry_count, max_retries, backoff.as_millis()
                        );
                        tokio::time::sleep(backoff).await;
                        last_error = Some(error_msg);
                    }
                } else {
                    return Err(format!("Non-retriable error in {}: {}", operation_name, error_msg).into());
                }
            }
        }
    }

    Err(format!("Failed {} after {} attempts. Last error: {}", 
        operation_name, max_retries, last_error.unwrap_or_else(|| "Unknown".to_string())).into())
}

/// Function that determines if an error is a retriable network error
pub fn is_network_error(error_msg: &str) -> bool {
    error_msg.contains("NETWORK_ERROR") || 
    error_msg.contains("SendRequest") ||
    error_msg.contains("Connection reset") ||
    error_msg.contains("timeout")
}

/// Calculate exponential backoff duration with jitter
fn get_backoff_duration(retry_count: usize) -> Duration {
    let base_ms = 250;
    let max_ms = 30_000; // 30 seconds max
    
    // Exponential backoff: 250ms, 500ms, 1s, 2s, 4s, etc.
    let exp_ms = base_ms * 2u64.pow(retry_count as u32);
    
    // Add jitter (±10%)
    let jitter_factor = 0.9 + (rand::random::<f64>() * 0.2);
    let with_jitter_ms = (exp_ms as f64 * jitter_factor) as u64;
    
    // Cap at max duration
    Duration::from_millis(with_jitter_ms.min(max_ms))
} 