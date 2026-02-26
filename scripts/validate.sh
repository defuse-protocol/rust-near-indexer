#!/usr/bin/env bash
#
# Validate indexed data in ClickHouse.
#
# Usage:
#   ./scripts/validate.sh                  # uses defaults from .env / localhost
#   ./scripts/validate.sh --url http://host:8123 --user admin --password secret --database mydb
#
# Exit codes:
#   0 — all checks passed
#   1 — one or more checks failed

set -euo pipefail

# ── Source .env if present ────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

# ── Defaults (match .env.example / docker-compose) ──────────────────────────
CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CH_USER="${CLICKHOUSE_USER:-indexer}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-indexer}"
CH_DATABASE="${CLICKHOUSE_DATABASE:-default}"

# ── CLI overrides ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --url)       CH_URL="$2";      shift 2 ;;
        --user)      CH_USER="$2";     shift 2 ;;
        --password)  CH_PASSWORD="$2"; shift 2 ;;
        --database)  CH_DATABASE="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

FAILURES=0

ch_query() {
    curl -sS "${CH_URL}/?database=${CH_DATABASE}" \
        --user "${CH_USER}:${CH_PASSWORD}" \
        --data-binary "$1"
}

# ── Helper: run a check, print result ───────────────────────────────────────
# check <name> <query> <expected_value>
#   Runs query, trims whitespace, compares to expected. Fails if mismatch.
check_eq() {
    local name="$1" query="$2" expected="$3"
    local result
    result="$(ch_query "$query" | tr -d '[:space:]')"
    if [[ "$result" == "$expected" ]]; then
        echo "  PASS  $name"
    else
        echo "  FAIL  $name (expected=$expected got=$result)"
        FAILURES=$((FAILURES + 1))
    fi
}

# check_zero <name> <query>
#   Expects the query to return 0.
check_zero() {
    check_eq "$1" "$2" "0"
}

# check_gt <name> <query> <min_exclusive>
#   Expects the numeric result to be greater than min_exclusive.
check_gt() {
    local name="$1" query="$2" min="$3"
    local result
    result="$(ch_query "$query" | tr -d '[:space:]')"
    if [[ "$result" =~ ^[0-9]+$ ]] && (( result > min )); then
        echo "  PASS  $name (count=$result)"
    else
        echo "  FAIL  $name (expected > $min, got=$result)"
        FAILURES=$((FAILURES + 1))
    fi
}

echo ""
echo "=== ClickHouse validation ($CH_URL, database=$CH_DATABASE) ==="
echo ""

# ── 1. Completeness: tables are not empty ───────────────────────────────────
echo "── Completeness ──"

check_gt "transactions table has rows" \
    "SELECT count(*) FROM transactions" 0

check_gt "receipts table has rows" \
    "SELECT count(*) FROM receipts" 0

check_gt "execution_outcomes table has rows" \
    "SELECT count(*) FROM execution_outcomes" 0

check_gt "events table has rows" \
    "SELECT count(*) FROM events" 0

# ── 2. Completeness: block height range consistency ─────────────────────────
echo ""
echo "── Block height ranges ──"

ch_query "
SELECT
    'transactions' AS t, count(*) AS rows, min(block_height) AS min_h, max(block_height) AS max_h
FROM transactions
UNION ALL
SELECT 'receipts', count(*), min(block_height), max(block_height) FROM receipts
UNION ALL
SELECT 'execution_outcomes', count(*), min(block_height), max(block_height) FROM execution_outcomes
UNION ALL
SELECT 'events', count(*), min(block_height), max(block_height) FROM events
FORMAT PrettyCompactMonoBlock
"

# ── 3. Completeness: null tx_hash in events ─────────────────────────────────
echo ""
echo "── Cache miss checks ──"

NULL_TX_COUNT="$(ch_query "SELECT count(*) FROM events WHERE tx_hash IS NULL" | tr -d '[:space:]')"
TOTAL_EVENTS="$(ch_query "SELECT count(*) FROM events" | tr -d '[:space:]')"
if [[ "$TOTAL_EVENTS" -gt 0 ]]; then
    # Use cross-multiplication to avoid integer division truncating to 0:
    # NULL_TX_COUNT / TOTAL_EVENTS <= 5/100  ⟺  NULL_TX_COUNT * 100 <= 5 * TOTAL_EVENTS
    if (( NULL_TX_COUNT * 100 <= 5 * TOTAL_EVENTS )); then
        NULL_PCT=$(awk "BEGIN { printf \"%.1f\", ($NULL_TX_COUNT * 100.0 / $TOTAL_EVENTS) }")
        echo "  PASS  events with null tx_hash: $NULL_TX_COUNT/$TOTAL_EVENTS (${NULL_PCT}%)"
    else
        NULL_PCT=$(awk "BEGIN { printf \"%.1f\", ($NULL_TX_COUNT * 100.0 / $TOTAL_EVENTS) }")
        echo "  FAIL  events with null tx_hash: $NULL_TX_COUNT/$TOTAL_EVENTS (${NULL_PCT}% — expected <= 5%)"
        FAILURES=$((FAILURES + 1))
    fi
else
    echo "  PASS  events with null tx_hash: 0/0 (no events)"
fi

# ── 4. Correctness: referential integrity ───────────────────────────────────
echo ""
echo "── Referential integrity ──"

check_zero "all receipt parent_tx exist in transactions" \
    "SELECT count(*) FROM receipts r LEFT JOIN transactions t ON r.parent_transaction_hash = t.transaction_hash WHERE t.transaction_hash IS NULL"

check_zero "all execution_outcome parent_tx exist in transactions" \
    "SELECT count(*) FROM execution_outcomes eo LEFT JOIN transactions t ON eo.parent_transaction_hash = t.transaction_hash WHERE t.transaction_hash IS NULL"

# ── 5. Correctness: events only from accounts of interest ──────────────────
echo ""
echo "── Account filtering ──"

check_zero "events only from accounts of interest" \
    "SELECT count(*) FROM events WHERE contract_id NOT IN ('intents.near', 'defuse-alpha.near', 'staging-intents.near')"

# ── 6. Correctness: valid JSON in serialized columns ───────────────────────
echo ""
echo "── JSON validity ──"

check_zero "transactions.actions is valid JSON" \
    "SELECT count(*) FROM transactions WHERE NOT isValidJSON(actions)"

check_zero "receipts.actions is valid JSON" \
    "SELECT count(*) FROM receipts WHERE NOT isValidJSON(actions)"

check_zero "execution_outcomes.logs is valid JSON" \
    "SELECT count(*) FROM execution_outcomes WHERE NOT isValidJSON(logs)"

# ── Summary ─────────────────────────────────────────────────────────────────
echo ""
if (( FAILURES == 0 )); then
    echo "=== ALL CHECKS PASSED ==="
    exit 0
else
    echo "=== $FAILURES CHECK(S) FAILED ==="
    exit 1
fi
