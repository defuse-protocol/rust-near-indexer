#!/usr/bin/env bash
#
# Cross-validate local Postgres (silver_dip4_transfers) against production
# ClickHouse (silver_dip4_transfer).
#
# Usage:
#   ./scripts/cross-validate-pg.sh \
#     --pg-url postgres://indexer:indexer@localhost:5432/defuse_indexer \
#     --prod-url https://prod:8443 --prod-user user --prod-password "$PW" --prod-database default \
#     --block-start 186929800 --block-end 186929900
#
# Exit codes:
#   0 — all checks pass
#   1 — one or more checks fail

set -euo pipefail

# ── Source .env if present ────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

# ── Defaults ──────────────────────────────────────────────────────────────────
PG_URL="${POSTGRES_URL:-postgres://indexer:indexer@localhost:5432/defuse_indexer}"

PROD_URL="${PROD_CLICKHOUSE_URL:-}"
PROD_USER="${PROD_USER:-}"
PROD_PASSWORD="${PROD_PASSWORD:-}"
PROD_DATABASE="${PROD_DATABASE:-default}"

BLOCK_START=""
BLOCK_END=""

# ── CLI args ──────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --pg-url)          PG_URL="$2";          shift 2 ;;
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

# Validate block range values are strictly numeric
if ! [[ "$BLOCK_START" =~ ^[0-9]+$ ]]; then
    echo "Error: --block-start must be a positive integer, got: $BLOCK_START"
    exit 1
fi
if ! [[ "$BLOCK_END" =~ ^[0-9]+$ ]]; then
    echo "Error: --block-end must be a positive integer, got: $BLOCK_END"
    exit 1
fi

FAILURES=0

# ── Query helpers ─────────────────────────────────────────────────────────────
pg_query() {
    psql "$PG_URL" -t -A -F$'\t' -c "$1"
}

ch_prod() {
    curl -sS "${PROD_URL}/?database=${PROD_DATABASE}" \
        --user "${PROD_USER}:${PROD_PASSWORD}" \
        --data-binary "$1"
}

# ── Column definitions ────────────────────────────────────────────────────────
# Shared columns between PG silver_dip4_transfers and CH silver_dip4_transfer
# (excludes PG-only 'referral')
#
# Normalization:
#  - NULLable text cols: COALESCE(col, '')
#  - block_timestamp: compared at microsecond precision (PG TIMESTAMPTZ is µs,
#    CH DateTime64(9) is ns). Both sides truncated to µs epoch for comparison.
#  - amount: excluded from text diff (Float64 vs NUMERIC serialization differs),
#    checked separately with relative tolerance

# PG normalized select list (excludes amount — compared separately)
# block_timestamp: extract epoch microseconds from TIMESTAMPTZ
PG_COLS="block_height::text,
  (EXTRACT(EPOCH FROM block_timestamp) * 1000000)::bigint::text,
  block_hash,
  tx_hash,
  contract_id,
  execution_status,
  version,
  standard,
  event,
  related_receipt_id,
  related_receipt_receiver_id,
  related_receipt_predecessor_id,
  COALESCE(memo, ''),
  COALESCE(old_owner_id, ''),
  COALESCE(new_owner_id, ''),
  COALESCE(token_id, ''),
  intent_hash"

# CH normalized select list (excludes amount — compared separately)
# block_timestamp: extract epoch microseconds (truncate ns → µs to match PG precision)
CH_COLS="toString(block_height),
  toString(toUnixTimestamp64Micro(block_timestamp)),
  block_hash,
  tx_hash,
  contract_id,
  execution_status,
  version,
  standard,
  event,
  related_receipt_id,
  related_receipt_receiver_id,
  related_receipt_predecessor_id,
  COALESCE(memo, ''),
  COALESCE(old_owner_id, ''),
  COALESCE(new_owner_id, ''),
  COALESCE(token_id, ''),
  intent_hash"

# Sort key for deterministic ordering
PG_SORT_KEY="block_height, related_receipt_id, event, COALESCE(old_owner_id, ''), COALESCE(new_owner_id, ''), COALESCE(token_id, '')"
CH_SORT_KEY="block_height, related_receipt_id, event, COALESCE(old_owner_id, ''), COALESCE(new_owner_id, ''), COALESCE(token_id, '')"

PG_BLOCK_FILTER="block_height >= ${BLOCK_START} AND block_height <= ${BLOCK_END}"
CH_BLOCK_FILTER="block_height >= ${BLOCK_START} AND block_height <= ${BLOCK_END}"

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "=== Cross-validation: PG vs prod CH ==="
echo "=== Block range: ${BLOCK_START} .. ${BLOCK_END} ==="
echo "=== Table: PG silver_dip4_transfers ↔ CH silver_dip4_transfer ==="
echo ""

# ── Phase 1: Total row count ─────────────────────────────────────────────────
echo "── Phase 1: Total row count ──"

pg_count="$(pg_query "SELECT count(*) FROM silver_dip4_transfers WHERE ${PG_BLOCK_FILTER};" | tr -d '[:space:]')"
ch_count="$(ch_prod "SELECT count() FROM silver_dip4_transfer FINAL WHERE ${CH_BLOCK_FILTER} FORMAT TSV" | tr -d '[:space:]')"

if [[ "$pg_count" == "$ch_count" ]]; then
    echo "  PASS  row count (both=${pg_count})"
else
    echo "  FAIL  row count (pg=${pg_count} ch=${ch_count})"
    FAILURES=$((FAILURES + 1))
fi

# ── Phase 2: Per-block row counts ────────────────────────────────────────────
echo ""
echo "── Phase 2: Per-block row counts ──"

pg_blocks="$(pg_query "SELECT block_height, count(*) FROM silver_dip4_transfers WHERE ${PG_BLOCK_FILTER} GROUP BY block_height ORDER BY block_height;" | tr -d '\r')"
ch_blocks="$(ch_prod "SELECT block_height, count() FROM silver_dip4_transfer FINAL WHERE ${CH_BLOCK_FILTER} GROUP BY block_height ORDER BY block_height FORMAT TSV" | tr -d '\r')"

block_diff="$(diff <(echo "$pg_blocks") <(echo "$ch_blocks") || true)"

if [[ -z "$block_diff" ]]; then
    echo "  PASS  per-block row counts match"
else
    echo "  FAIL  per-block row counts differ:"
    echo "$block_diff" | head -40 | sed 's/^/        /'
    FAILURES=$((FAILURES + 1))
fi

# ── Phase 3: Row-level content comparison (excluding amount) ─────────────────
echo ""
echo "── Phase 3: Row-level content diff (excl. amount) ──"

pg_rows="$(pg_query "SELECT ${PG_COLS} FROM silver_dip4_transfers WHERE ${PG_BLOCK_FILTER} ORDER BY ${PG_SORT_KEY};" | tr -d '\r')"
ch_rows="$(ch_prod "SELECT ${CH_COLS} FROM silver_dip4_transfer FINAL WHERE ${CH_BLOCK_FILTER} ORDER BY ${CH_SORT_KEY} FORMAT TSV" | tr -d '\r')"

content_diff="$(diff <(echo "$pg_rows") <(echo "$ch_rows") || true)"

if [[ -z "$content_diff" ]]; then
    echo "  PASS  all row contents match (excl. amount)"
else
    echo "  FAIL  row contents differ:"
    echo "$content_diff" | head -60 | sed 's/^/        /'
    FAILURES=$((FAILURES + 1))
fi

# ── Phase 4: Amount comparison (relative tolerance for Float64 vs NUMERIC) ───
echo ""
echo "── Phase 4: Amount comparison ──"
echo "  (CH stores Float64, PG stores NUMERIC — checking relative error < 1e-10)"

# Get amounts from PG: block_height, receipt_id, token_id, amount as text
pg_amounts="$(pg_query "SELECT block_height, related_receipt_id, COALESCE(token_id, ''), amount::text FROM silver_dip4_transfers WHERE ${PG_BLOCK_FILTER} ORDER BY ${PG_SORT_KEY};" | tr -d '\r')"
# Get amounts from CH (decimal string)
ch_amounts="$(ch_prod "SELECT block_height, related_receipt_id, COALESCE(token_id, ''), toString(amount) FROM silver_dip4_transfer FINAL WHERE ${CH_BLOCK_FILTER} ORDER BY ${CH_SORT_KEY} FORMAT TSV" | tr -d '\r')"

amount_mismatches=0
while IFS=$'\t' read -r pg_bh pg_rid pg_tid pg_amt; do
    # Read corresponding CH line
    IFS=$'\t' read -r ch_bh ch_rid ch_tid ch_amt <&3 || true

    if [[ "$pg_bh" != "$ch_bh" || "$pg_rid" != "$ch_rid" || "$pg_tid" != "$ch_tid" ]]; then
        echo "  FAIL  row key mismatch: pg=(${pg_bh},${pg_rid},${pg_tid}) ch=(${ch_bh},${ch_rid},${ch_tid})"
        amount_mismatches=$((amount_mismatches + 1))
        continue
    fi

    if [[ -z "$pg_amt" && -z "$ch_amt" ]]; then
        continue  # both null
    fi

    # Use awk to check relative error
    rel_err="$(echo "$pg_amt $ch_amt" | awk '{
        pg = $1 + 0; ch = $2 + 0;
        if (pg == 0 && ch == 0) { print "0"; exit }
        denom = (pg > 0 ? pg : -pg);
        if (denom == 0) denom = (ch > 0 ? ch : -ch);
        diff = pg - ch; if (diff < 0) diff = -diff;
        print diff / denom
    }')"

    ok="$(echo "$rel_err" | awk '{ print ($1 < 1e-10) ? "1" : "0" }')"
    if [[ "$ok" != "1" ]]; then
        echo "  FAIL  block=${pg_bh} receipt=${pg_rid} token=${pg_tid}: pg_amount=${pg_amt} ch_amount=${ch_amt} rel_err=${rel_err}"
        amount_mismatches=$((amount_mismatches + 1))
    fi
done <<< "$pg_amounts" 3<<< "$ch_amounts"

if (( amount_mismatches == 0 )); then
    echo "  PASS  all amounts match within tolerance"
else
    echo "  FAIL  ${amount_mismatches} amount mismatch(es)"
    FAILURES=$((FAILURES + 1))
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
if (( FAILURES == 0 )); then
    echo "=== ALL CHECKS PASS ==="
    exit 0
else
    echo "=== ${FAILURES} CHECK(S) FAILED ==="
    exit 1
fi
