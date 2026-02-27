use std::collections::HashMap;

use crate::types::{EventRow, SilverDip4TransferRow};

/// Production accounts for silver_dip4_transfer (same filter as the ClickHouse MV).
const PRODUCTION_CONTRACT_IDS: &[&str] = &["defuse-alpha.near", "intents.near"];

/// Typed representation of a single item in a DIP-4 `transfer` event's data array.
#[derive(serde::Deserialize)]
struct TransferItem {
    memo: Option<String>,
    #[serde(rename = "account_id")]
    old_owner_id: Option<String>,
    #[serde(rename = "receiver_id")]
    new_owner_id: Option<String>,
    #[serde(default)]
    intent_hash: String,
    #[serde(default)]
    tokens: serde_json::Map<String, serde_json::Value>,
}

/// Extract denormalized silver DIP-4 transfer rows from raw event rows.
///
/// Mirrors the ClickHouse materialized view `mv_silver_dip4_transfer`:
/// 1. Build a referral lookup from `dip4` / `token_diff` events.
/// 2. Parse `dip4` / `transfer` events, flatten `tokens` map, join referral.
/// 3. Filter to production contract IDs only.
pub fn extract_silver_dip4_transfers(events: &[EventRow]) -> Vec<SilverDip4TransferRow> {
    // Step 1: Build referral lookup keyed by related_receipt_id.
    // A token_diff event's data array may contain items with a `referral` field.
    // We take the first non-null referral per receipt.
    let mut referral_by_receipt: HashMap<&str, String> = HashMap::new();
    for event in events {
        if event.standard != "dip4" || event.event != "token_diff" {
            continue;
        }
        if !PRODUCTION_CONTRACT_IDS.contains(&event.contract_id.as_str()) {
            continue;
        }
        if referral_by_receipt.contains_key(event.related_receipt_id.as_str()) {
            continue;
        }
        if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(&event.data) {
            for item in &arr {
                if let Some(referral) = item.get("referral").and_then(|v| v.as_str())
                    && !referral.is_empty()
                {
                    referral_by_receipt.insert(&event.related_receipt_id, referral.to_string());
                    break;
                }
            }
        }
    }

    // Step 2: Parse transfer events and flatten tokens map.
    let mut rows = Vec::new();
    for event in events {
        if event.standard != "dip4" || event.event != "transfer" {
            continue;
        }
        if !PRODUCTION_CONTRACT_IDS.contains(&event.contract_id.as_str()) {
            continue;
        }

        let data_arr: Vec<serde_json::Value> = match serde_json::from_str(&event.data) {
            Ok(arr) => arr,
            Err(_) => continue,
        };

        let referral = referral_by_receipt
            .get(event.related_receipt_id.as_str())
            .cloned();

        for item in &data_arr {
            let item: TransferItem = match serde_json::from_value(item.clone()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            // Filter empty memo to None (matches old behavior)
            let memo = item.memo.filter(|s| !s.is_empty());

            for (token_id, amount_val) in &item.tokens {
                let amount = match amount_val.as_str() {
                    Some(s) => s.to_string(),
                    None => amount_val.to_string().trim_matches('"').to_string(),
                };

                rows.push(SilverDip4TransferRow {
                    block_height: event.block_height,
                    block_timestamp: event.block_timestamp,
                    block_hash: event.block_hash.clone(),
                    tx_hash: event.tx_hash.clone().unwrap_or_default(),
                    contract_id: event.contract_id.clone(),
                    execution_status: event.execution_status.clone(),
                    version: event.version.clone(),
                    standard: event.standard.clone(),
                    event: event.event.clone(),
                    related_receipt_id: event.related_receipt_id.clone(),
                    related_receipt_receiver_id: event.related_receipt_receiver_id.clone(),
                    related_receipt_predecessor_id: event.related_receipt_predecessor_id.clone(),
                    memo: memo.clone(),
                    old_owner_id: item.old_owner_id.clone(),
                    new_owner_id: item.new_owner_id.clone(),
                    token_id: token_id.clone(),
                    amount,
                    intent_hash: item.intent_hash.clone(),
                    referral: referral.clone(),
                });
            }
        }
    }

    rows
}
