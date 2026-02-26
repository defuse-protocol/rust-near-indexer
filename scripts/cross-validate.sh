#!/usr/bin/env bash
#
# Cross-validate local indexer data against production ClickHouse.
#
# Compares aggregate fingerprints (row counts + order-independent checksums)
# for a shared block range. On mismatch, drills down to per-block counts and
# sample rows for manual inspection.
#
# Usage:
#   ./scripts/cross-validate.sh \
#     --local-url http://localhost:8123 --local-user indexer --local-password indexer --local-database default \
#     --prod-url https://prod:8443 --prod-user user --prod-password "$PW" --prod-database default \
#     --block-start 168545460 --block-end 168545523
#
# Exit codes:
#   0 — all tables match
#   1 — one or more tables diverge

set -euo pipefail

# ── Source .env if present ────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

# ── Defaults (local side reads from env vars, matching validate.sh / .env) ────
LOCAL_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
LOCAL_USER="${CLICKHOUSE_USER:-indexer}"
LOCAL_PASSWORD="${CLICKHOUSE_PASSWORD:-indexer}"
LOCAL_DATABASE="${CLICKHOUSE_DATABASE:-default}"

PROD_URL="${PROD_CLICKHOUSE_URL:-}"
PROD_USER="${PROD_USER:-}"
PROD_PASSWORD="${PROD_PASSWORD:-}"
PROD_DATABASE="${PROD_DATABASE:-default}"

BLOCK_START=""
BLOCK_END=""

# ── CLI args ──────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --local-url)       LOCAL_URL="$2";       shift 2 ;;
        --local-user)      LOCAL_USER="$2";      shift 2 ;;
        --local-password)  LOCAL_PASSWORD="$2";  shift 2 ;;
        --local-database)  LOCAL_DATABASE="$2";  shift 2 ;;
        --prod-url)        PROD_URL="$2";        shift 2 ;;
        --prod-user)       PROD_USER="$2";       shift 2 ;;
        --prod-password)   PROD_PASSWORD="$2";   shift 2 ;;
        --prod-database)   PROD_DATABASE="$2";   shift 2 ;;
        --block-start)     BLOCK_START="$2";     shift 2 ;;
        --block-end)       BLOCK_END="$2";       shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

if [[ -z "$PROD_URL" || -z "$BLOCK_START" || -z "$BLOCK_END" ]]; then
    echo "Required: --prod-url, --block-start, --block-end"
    echo "Run with --help or see script header for full usage."
    exit 1
fi

# Validate block range values are strictly numeric (prevent SQL injection)
if ! [[ "$BLOCK_START" =~ ^[0-9]+$ ]]; then
    echo "Error: --block-start must be a positive integer, got: $BLOCK_START"
    exit 1
fi
if ! [[ "$BLOCK_END" =~ ^[0-9]+$ ]]; then
    echo "Error: --block-end must be a positive integer, got: $BLOCK_END"
    exit 1
fi

if (( BLOCK_START > BLOCK_END )); then
    echo "Error: --block-start ($BLOCK_START) must be <= --block-end ($BLOCK_END)"
    exit 1
fi

FAILURES=0

# ── Query helpers ─────────────────────────────────────────────────────────────
ch_local() {
    curl -sS "${LOCAL_URL}/?database=${LOCAL_DATABASE}" \
        --user "${LOCAL_USER}:${LOCAL_PASSWORD}" \
        --data-binary "$1"
}

ch_prod() {
    curl -sS "${PROD_URL}/?database=${PROD_DATABASE}" \
        --user "${PROD_USER}:${PROD_PASSWORD}" \
        --data-binary "$1"
}

BLOCK_FILTER="block_height >= ${BLOCK_START} AND block_height <= ${BLOCK_END}"

# ── Fingerprint comparison ────────────────────────────────────────────────────
# compare_table <table_name> <pk_cols_csv> <all_cols_csv>
#   pk_cols_csv  — comma-separated PK columns for cityHash64
#   all_cols_csv — comma-separated all columns for cityHash64
compare_table() {
    local table="$1" pk_cols="$2" all_cols="$3"

    local query="
        SELECT
            count()                                       AS row_count,
            groupBitXor(cityHash64(${pk_cols}))           AS pk_xor,
            groupBitXor(cityHash64(${all_cols}))          AS row_xor
        FROM ${table} FINAL
        WHERE ${BLOCK_FILTER}
        FORMAT TSV
    "

    local local_result prod_result
    local_result="$(ch_local "$query" | tr -d '\r')"
    prod_result="$(ch_prod "$query" | tr -d '\r')"

    local local_count local_pk local_row
    local_count="$(echo "$local_result" | cut -f1)"
    local_pk="$(echo "$local_result" | cut -f2)"
    local_row="$(echo "$local_result" | cut -f3)"

    local prod_count prod_pk prod_row
    prod_count="$(echo "$prod_result" | cut -f1)"
    prod_pk="$(echo "$prod_result" | cut -f2)"
    prod_row="$(echo "$prod_result" | cut -f3)"

    local status="PASS"
    local details=""

    if [[ "$local_count" != "$prod_count" ]]; then
        status="FAIL"
        details+=" count(local=${local_count} prod=${prod_count})"
    fi
    if [[ "$local_pk" != "$prod_pk" ]]; then
        status="FAIL"
        details+=" pk_xor_mismatch"
    fi
    if [[ "$local_row" != "$prod_row" ]]; then
        status="FAIL"
        details+=" row_xor_mismatch"
    fi

    if [[ "$status" == "PASS" ]]; then
        echo "  PASS  ${table} (rows=${local_count})"
    else
        echo "  FAIL  ${table}${details}"
        FAILURES=$((FAILURES + 1))
        drill_down "$table" "$all_cols"
    fi
}

# ── Drill-down on mismatch ───────────────────────────────────────────────────
drill_down() {
    local table="$1" all_cols="$2"

    echo ""
    echo "        ── Drill-down: ${table} ──"
    echo ""

    # Per-block row counts from both sides
    local block_query="
        SELECT block_height, count() AS cnt
        FROM ${table} FINAL
        WHERE ${BLOCK_FILTER}
        GROUP BY block_height
        ORDER BY block_height
        FORMAT TSV
    "

    local local_blocks prod_blocks
    local_blocks="$(ch_local "$block_query" | tr -d '\r')"
    prod_blocks="$(ch_prod "$block_query" | tr -d '\r')"

    # Find blocks that differ
    local diff_blocks
    diff_blocks="$(diff <(echo "$local_blocks") <(echo "$prod_blocks") || true)"

    if [[ -n "$diff_blocks" ]]; then
        echo "        Block-level row count differences:"
        echo "$diff_blocks" | head -40 | sed 's/^/          /'
        echo ""
    fi

    # Even if counts match, content may differ — check per-block row_xor
    local xor_query="
        SELECT block_height,
               count() AS cnt,
               groupBitXor(cityHash64(${all_cols})) AS row_xor
        FROM ${table} FINAL
        WHERE ${BLOCK_FILTER}
        GROUP BY block_height
        ORDER BY block_height
        FORMAT TSV
    "

    local local_xor prod_xor
    local_xor="$(ch_local "$xor_query" | tr -d '\r')"
    prod_xor="$(ch_prod "$xor_query" | tr -d '\r')"

    local xor_diff
    xor_diff="$(diff <(echo "$local_xor") <(echo "$prod_xor") || true)"

    if [[ -n "$xor_diff" ]]; then
        echo "        Per-block fingerprint differences (block_height, count, row_xor):"
        echo "$xor_diff" | head -40 | sed 's/^/          /'
        echo ""

        # Extract first diverging block height for sample rows
        local first_bad_block
        first_bad_block="$(echo "$xor_diff" | grep -E '^[<>]' | head -1 | awk '{print $2}')"

        if [[ -n "$first_bad_block" ]] && [[ "$first_bad_block" =~ ^[0-9]+$ ]]; then
            echo "        Sample rows from block ${first_bad_block} (local, top 20):"
            ch_local "SELECT ${all_cols} FROM ${table} FINAL WHERE block_height = ${first_bad_block} ORDER BY ${all_cols} LIMIT 20 FORMAT PrettyCompactMonoBlock" \
                | sed 's/^/          /'
            echo ""
            echo "        Sample rows from block ${first_bad_block} (prod, top 20):"
            ch_prod "SELECT ${all_cols} FROM ${table} FINAL WHERE block_height = ${first_bad_block} ORDER BY ${all_cols} LIMIT 20 FORMAT PrettyCompactMonoBlock" \
                | sed 's/^/          /'
            echo ""
        fi
    fi
}

# ── Events: extra cache-miss comparison ───────────────────────────────────────
compare_events_cache_miss() {
    local query="
        SELECT countIf(tx_hash IS NULL) AS null_tx
        FROM events FINAL
        WHERE ${BLOCK_FILTER}
        FORMAT TSV
    "

    local local_null prod_null
    local_null="$(ch_local "$query" | tr -d '[:space:]')"
    prod_null="$(ch_prod "$query" | tr -d '[:space:]')"

    if [[ "$local_null" == "$prod_null" ]]; then
        echo "  PASS  events.tx_hash NULL count (both=${local_null})"
    else
        echo "  FAIL  events.tx_hash NULL count (local=${local_null} prod=${prod_null})"
        FAILURES=$((FAILURES + 1))
    fi
}

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "=== Cross-validation: local ($LOCAL_URL) vs prod ($PROD_URL) ==="
echo "=== Block range: ${BLOCK_START} .. ${BLOCK_END} ==="
echo ""

# ── Phase 1: Core tables ─────────────────────────────────────────────────────
echo "── Core tables ──"

compare_table "transactions" \
    "block_height, transaction_hash" \
    "block_height, block_timestamp, block_hash, transaction_hash, signer_id, receiver_id, actions"

compare_table "receipts" \
    "block_height, receipt_id" \
    "block_height, block_timestamp, block_hash, parent_transaction_hash, receipt_id, receiver_id, predecessor_id, actions"

compare_table "execution_outcomes" \
    "block_height, execution_outcome_id" \
    "block_height, block_timestamp, block_hash, parent_transaction_hash, executor_id, arrayStringConcat(arraySort(receipt_ids), ','), status, logs, tokens_burnt, gas_burnt, execution_outcome_id"

compare_table "events" \
    "block_height, related_receipt_id, index_in_log" \
    "block_height, block_timestamp, block_hash, contract_id, execution_status, version, standard, index_in_log, event, data, related_receipt_id, related_receipt_receiver_id, related_receipt_predecessor_id, tx_hash, receipt_index_in_block"

compare_events_cache_miss

# ── Phase 2: Silver tables ───────────────────────────────────────────────────
echo ""
echo "── Silver tables ──"

compare_table "silver_nep_245_events" \
    "block_height, related_receipt_id, event, old_owner_id, new_owner_id, token_id" \
    "block_height, block_timestamp, block_hash, tx_hash, contract_id, execution_status, version, standard, event, related_receipt_id, related_receipt_receiver_id, related_receipt_predecessor_id, memo, old_owner_id, new_owner_id, token_id, amount"

compare_table "silver_dip4_token_diff" \
    "block_height, related_receipt_id, intent_hash" \
    "block_height, block_timestamp, block_hash, contract_id, execution_status, version, standard, event, related_receipt_id, related_receipt_receiver_id, related_receipt_predecessor_id, account_id, diff_positive_token, diff_positive_amount, diff_negative_token, diff_negative_amount, intent_hash, referral"

compare_table "silver_dip4_public_keys" \
    "block_height, related_receipt_id, account_id" \
    "block_height, block_timestamp, block_hash, contract_id, execution_status, version, standard, event, related_receipt_id, related_receipt_receiver_id, related_receipt_predecessor_id, account_id, public_key"

compare_table "silver_dip4_intents_executed" \
    "block_height, related_receipt_id, intent_hash" \
    "block_height, block_timestamp, block_hash, contract_id, execution_status, version, standard, event, related_receipt_id, related_receipt_receiver_id, related_receipt_predecessor_id, account_id, intent_hash"

compare_table "silver_dip4_fee_changed" \
    "block_height, related_receipt_id" \
    "block_height, block_timestamp, block_hash, contract_id, execution_status, version, standard, event, related_receipt_id, related_receipt_receiver_id, related_receipt_predecessor_id, old_fee, new_fee"

compare_table "silver_dip4_transfer" \
    "block_height, related_receipt_id, event, old_owner_id, new_owner_id, token_id" \
    "block_height, block_timestamp, block_hash, tx_hash, contract_id, execution_status, version, standard, event, related_receipt_id, related_receipt_receiver_id, related_receipt_predecessor_id, memo, old_owner_id, new_owner_id, token_id, amount, intent_hash"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
if (( FAILURES == 0 )); then
    echo "=== ALL TABLES MATCH ==="
    exit 0
else
    echo "=== ${FAILURES} TABLE(S) DIVERGE ==="
    exit 1
fi
