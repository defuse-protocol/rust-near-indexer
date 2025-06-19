use crate::types::{EventJson, EventRow, ReceiptsCache, ReceiptsCacheArc, ReceiptOrDataId};
use crate::retry::{with_retry, is_network_error};
use clickhouse::Client;
use futures::StreamExt;
use near_lake_framework::LakeConfig;
use near_lake_framework::near_indexer_primitives::{self, StreamerMessage, views::ExecutionStatusView};
use serde_json::from_str;
use tokio_stream::wrappers::ReceiverStream;
use std::sync::Arc;
use tokio::sync::Mutex;
use redis::Client as RedisClient;
use futures::stream::FuturesUnordered;

const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";
const CACHE_SIZE: usize = 10000;
const CACHE_EXPIRATION_BLOCKS: u64 = 50;

pub async fn handle_stream(config: LakeConfig, client: Client, redis_client: Option<RedisClient>) {
    let (_, stream) = near_lake_framework::streamer(config);
    
    let receipts_cache = match redis_client {
        Some(redis) => {
            match ReceiptsCache::with_redis(CACHE_SIZE, redis, CACHE_EXPIRATION_BLOCKS) {
                Ok(cache) => {
                    let cache_arc = Arc::new(Mutex::new(cache));
                    
                    {
                        let mut cache_lock = cache_arc.lock().await;
                        if let Err(e) = cache_lock.load_from_redis().await {
                            eprintln!("Error loading cache from Redis: {}", e);
                        }
                    }
                    
                    println!("Using Redis for transaction cache persistence");
                    cache_arc
                },
                Err(e) => {
                    eprintln!("Failed to initialize Redis cache: {}, falling back to in-memory cache", e);
                    Arc::new(Mutex::new(ReceiptsCache::new(CACHE_SIZE)))
                }
            }
        },
        None => {
            println!("Using in-memory cache only");
            Arc::new(Mutex::new(ReceiptsCache::new(CACHE_SIZE)))
        },
    };

    let mut stream = ReceiverStream::new(stream);
    
    while let Some(message) = stream.next().await {
        let client_ref = &client;
        let cache_ref = receipts_cache.clone();
        let block_height = message.block.header.height;
        
        if let Err(e) = with_retry(
            || async { handle_streamer_message(message.clone(), client_ref, cache_ref.clone()).await },
            3,
            |e| is_network_error(&e.to_string()),
            &format!("process block {}", block_height)
        ).await {
            eprintln!("Failed to process block {}: {}", block_height, e);
        }
    }
}

async fn handle_streamer_message(
    message: StreamerMessage, 
    client: &Client, 
    receipts_cache: ReceiptsCacheArc
) -> Result<(), Box<dyn std::error::Error>> {
    let header = &message.block.header;
    println!("Processing block {}", header.height);
    
    let mut receipt_to_tx_hash = std::collections::HashMap::new();
    
    for shard in &message.shards {
        if let Some(chunk) = &shard.chunk {
            for tx in &chunk.transactions {
                let tx_hash = tx.transaction.hash.to_string();
                
                for receipt_id in &tx.outcome.execution_outcome.outcome.receipt_ids {
                    receipt_to_tx_hash.insert(receipt_id.to_string(), tx_hash.clone());
                }
            }
        }
    }
    
    {
        let mut cache_lock = receipts_cache.lock().await;
        cache_lock.update_block_height(header.height);
        
        for shard in &message.shards {
            if let Some(chunk) = &shard.chunk {
                for transaction_with_outcome in &chunk.transactions {
                    let tx_hash = transaction_with_outcome.transaction.hash.to_string();
                    
                    cache_lock.cache_tx_hash_trace(
                        &transaction_with_outcome.transaction.signer_id.to_string(),
                        &transaction_with_outcome.transaction.public_key.to_string(),
                        header.height,
                        tx_hash,
                    ).await;
                }
            }
        }
    }
    
    let mut rows = Vec::new();
    for shard in &message.shards {
        let chunk = Option::as_ref(&shard.chunk);
        let header = message.block.header.clone();
        
        let receipts_cache_clone = receipts_cache.clone();
        let futures: FuturesUnordered<_> = shard.receipt_execution_outcomes.iter().map(|outcome| {
            let receipts_cache_clone = receipts_cache_clone.clone();
            let _chunk = chunk.cloned();
            let header = header.clone();
            let outcome = outcome.clone();
            let receipt_to_tx_hash = receipt_to_tx_hash.clone();
            
            async move {
                let receipt_id_str = outcome.receipt.receipt_id.to_string();
                
                let tx_hash = if outcome.receipt.predecessor_id != "system" {
                    if let Some(tx_hash) = receipt_to_tx_hash.get(&receipt_id_str) {
                        tx_hash.clone()
                    } else {
                        let mut cache_lock = receipts_cache_clone.lock().await;
                        let receipt_id = ReceiptOrDataId::ReceiptId(outcome.receipt.receipt_id);
                        
                        match cache_lock.cache_get(&receipt_id) {
                            Some(hash_str) => hash_str.clone(),
                            None => {
                                if let near_indexer_primitives::views::ReceiptEnumView::Action { signer_id, signer_public_key, .. } = &outcome.receipt.receipt {
                                    if let Some(tx_hash) = cache_lock.trace_tx_hash(
                                        &signer_id.to_string(),
                                        &signer_public_key.to_string(),
                                        header.height,
                                    ).await {
                                        cache_lock.cache_set(receipt_id, tx_hash.clone());
                                        tx_hash
                                    } else {
                                        // NOTE: Silent fail; easily detectable
                                        "".to_string()
                                    }
                                } else {
                                    // NOTE: Silent fail; easily detectable
                                    "".to_string()
                                }
                            }
                        }
                    }
                } else {
                    "".to_string()
                };

                outcome
                    .execution_outcome
                    .outcome
                    .logs
                    .iter()
                    .enumerate()
                    .filter_map(|(index_in_log, log)| 
                        parse_event(
                            index_in_log,
                            log,
                            tx_hash.clone(),
                            &outcome,
                            &header
                        )
                    )
                    .collect::<Vec<_>>()
            }
        }).collect();

        let mut event_rows = futures.collect::<Vec<_>>().await;
        for row_vec in event_rows.drain(..) {
            rows.extend(row_vec);
        }
    }

    // Insert all rows into the database
    if !rows.is_empty() {
        crate::database::insert_rows(client, &rows).await?;
    }
    
    Ok(())
}

fn parse_event(
    index_in_log: usize,
    log: &str,
    tx_hash: String,
    outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    header: &near_indexer_primitives::views::BlockHeaderView,
) -> Option<EventRow> {
    let log_trimmed = log.trim();

    if log_trimmed.starts_with(EVENT_JSON_PREFIX) {
        if let Ok(event) = from_str::<EventJson>(&log_trimmed[EVENT_JSON_PREFIX.len()..]) {
            let contract_id = &outcome.execution_outcome.outcome.executor_id.to_string();
            if contract_id == "intents.near" || contract_id == "defuse-alpha.near" || contract_id == "staging-intents.near" {
                if log_trimmed.contains("dip4") || log_trimmed.contains("nep245") {
                    println!("Event: {}", log_trimmed);
                    return Some(EventRow {
                        block_height: header.height,
                        block_timestamp: header.timestamp,
                        block_hash: header.hash.to_string(),
                        contract_id: contract_id.to_string(),
                        execution_status: parse_status(outcome.execution_outcome.outcome.status.clone()),
                        version: event.version,
                        standard: event.standard,
                        index_in_log: index_in_log as u64,
                        event: event.event,
                        data:  event.data.to_string(),
                        related_receipt_id: outcome.receipt.receipt_id.to_string(),
                        related_receipt_receiver_id: outcome.receipt.receiver_id.to_string(),
                        related_receipt_predecessor_id: outcome.receipt.predecessor_id.to_string(),
                        tx_hash: Some(tx_hash),
                    });
                }
            }
        }
    }
    None
}

fn parse_status(status: ExecutionStatusView) -> String {
    match status {
        ExecutionStatusView::SuccessReceiptId(_) => "success_receipt_id".to_string(),
        ExecutionStatusView::SuccessValue(_) => "success_value".to_string(),
        ExecutionStatusView::Unknown => "unknown".to_string(),
        ExecutionStatusView::Failure(_) => "failure".to_string(),
    }
}
