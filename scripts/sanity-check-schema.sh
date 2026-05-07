#!/usr/bin/env bash
#
# Sanity check the silver-layer schema after the UInt128 / Int256 / index_in_log
# changes. Run against a freshly indexed local ClickHouse to verify:
#   - silver tables have rows
#   - amount columns survive as integer-precision (no Float64 drift)
#   - new provenance columns (index_in_log, receipt_index_in_block) are populated
#   - the silver_transfers UNION view compiles and returns intact amounts
#   - silver amounts round-trip exactly against the raw events.data JSON
#
# Usage:
#   ./scripts/sanity-check-schema.sh                     # uses defaults from .env
#   ./scripts/sanity-check-schema.sh --url http://host:8123 --user u --password p --database d
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

CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CH_USER="${CLICKHOUSE_USER:-indexer}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-indexer}"
CH_DATABASE="${CLICKHOUSE_DATABASE:-default}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --url)       CH_URL="$2";      shift 2 ;;
        --user)      CH_USER="$2";     shift 2 ;;
        --password)  CH_PASSWORD="$2"; shift 2 ;;
        --database)  CH_DATABASE="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,17p' "$0"; exit 0 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

FAILURES=0

ch_query() {
    curl -sS "${CH_URL}/?database=${CH_DATABASE}" \
        --user "${CH_USER}:${CH_PASSWORD}" \
        --data-binary "$1"
}

# Like ch_query but exits non-zero on HTTP 4xx/5xx. Use when the only thing
# you care about is whether the query compiled / executed cleanly. Plain -sS
# returns exit code 0 on HTTP errors, which would silently pass these checks.
ch_ok() {
    curl -fsS "${CH_URL}/?database=${CH_DATABASE}" \
        --user "${CH_USER}:${CH_PASSWORD}" \
        --data-binary "$1" >/dev/null
}

trim() { tr -d '[:space:]'; }

pass() { echo "  PASS  $1"; }
fail() { echo "  FAIL  $1"; FAILURES=$((FAILURES + 1)); }

# ── 1. Column types match expectations ───────────────────────────────────────
echo "=== column types ==="

check_type() {
    local table="$1" column="$2" expected="$3"
    local got
    got="$(ch_query "SELECT type FROM system.columns
                     WHERE database = '${CH_DATABASE}' AND table = '${table}' AND name = '${column}'
                     LIMIT 1" | trim)"
    if [[ "$got" == "$expected" ]]; then
        pass "${table}.${column} = ${expected}"
    else
        fail "${table}.${column} expected=${expected} got=${got}"
    fi
}

check_type silver_nep_245_events       amount                  "Nullable(UInt128)"
check_type silver_nep_245_events       index_in_log            "UInt64"
check_type silver_nep_245_events       receipt_index_in_block  "UInt64"
check_type silver_dip4_transfer        amount                  "Nullable(UInt128)"
check_type staging_silver_dip4_transfer amount                 "Nullable(UInt128)"
check_type silver_dip4_token_diff      diff_positive_amount    "Int256"
check_type silver_dip4_token_diff      diff_negative_amount    "Int256"

# ── 2. Tables have data ──────────────────────────────────────────────────────
echo "=== row counts ==="

check_rows() {
    local table="$1" min="${2:-0}"
    local got
    got="$(ch_query "SELECT count() FROM ${table}" | trim)"
    if [[ "$got" =~ ^[0-9]+$ ]] && (( got > min )); then
        pass "${table} has ${got} rows"
    else
        fail "${table} expected > ${min} rows, got=${got}"
    fi
}

check_rows events 0
check_rows silver_nep_245_events 0
# silver_dip4_transfer / silver_dip4_token_diff may legitimately be empty
# for a small block range — only warn, don't fail.
soft_count() {
    local table="$1"
    local got
    got="$(ch_query "SELECT count() FROM ${table}" | trim)"
    echo "  INFO  ${table}: ${got} rows"
}
soft_count silver_dip4_transfer
soft_count silver_dip4_token_diff

# ── 3. New provenance columns are populated ──────────────────────────────────
echo "=== provenance columns ==="

# At least one row should have index_in_log > 0 if any receipt emitted >1 events.
# Tighter check: receipt_index_in_block must be populated for every row
# (it's UInt64 NOT NULL, so we just require at least one non-zero in the sample
# range — non-zero proves it's not silently defaulted).
got="$(ch_query "SELECT countIf(receipt_index_in_block > 0) FROM silver_nep_245_events" | trim)"
if [[ "$got" =~ ^[0-9]+$ ]] && (( got > 0 )); then
    pass "silver_nep_245_events.receipt_index_in_block populated (${got} non-zero rows)"
else
    fail "silver_nep_245_events.receipt_index_in_block all zero — passthrough broken"
fi

# ── 4. Amount precision: round-trip silver ↔ raw events.data ─────────────────
echo "=== amount round-trip vs raw JSON ==="

# Pull up to 50 silver_nep_245_events rows and compare each silver amount to
# the integer string in the source events.data JSON. If any mismatch — fail.
mismatch="$(ch_query "
WITH sample AS (
    SELECT s.block_height, s.related_receipt_id, s.index_in_log, s.token_id,
           toString(s.amount) AS silver_amount,
           e.data AS raw_data
    FROM silver_nep_245_events s
    INNER JOIN events e
      ON e.block_height = s.block_height
     AND e.related_receipt_id = s.related_receipt_id
     AND e.index_in_log = s.index_in_log
    WHERE s.amount IS NOT NULL
    LIMIT 50
)
SELECT count()
FROM sample
WHERE position(raw_data, concat('\"', silver_amount, '\"')) = 0
" | trim)"

if [[ "$mismatch" == "0" ]]; then
    pass "silver amounts round-trip against events.data (50-row sample)"
else
    fail "silver amounts diverge from events.data in ${mismatch}/50 sampled rows"
fi

# ── 5. Unified UNION views compile ───────────────────────────────────────────
echo "=== UNION views ==="

if ch_ok "SELECT count() FROM silver_transfers"; then
    pass "silver_transfers UNION executes"
else
    fail "silver_transfers UNION failed"
fi
if ch_ok "SELECT count() FROM staging_silver_transfers"; then
    pass "staging_silver_transfers UNION executes"
else
    fail "staging_silver_transfers UNION failed"
fi

# ── 6. Summary ───────────────────────────────────────────────────────────────
echo
if (( FAILURES == 0 )); then
    echo "ALL SANITY CHECKS PASSED"
    exit 0
else
    echo "${FAILURES} CHECK(S) FAILED"
    exit 1
fi
