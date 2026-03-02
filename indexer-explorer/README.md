# indexer-explorer

Indexes DIP-4 transfer events from NEAR blockchain into PostgreSQL. Part of the Defuse custom indexer workspace.

## How to run

```bash
# Start local PostgreSQL (schema auto-applied via sqlx migrations)
docker compose up -d postgres --wait

# Configure environment
cp .env.example .env  # edit BLOCKSAPI_SERVER_ADDR, BLOCKSAPI_TOKEN, DATABASE_URL

# Build and run
cargo run --release -p indexer-explorer
```

## Table: `silver_dip4_transfers`

Stores DIP-4 transfer events extracted from NEAR receipts. Each row represents a single token movement (old_owner → new_owner) with amount, intent hash, and execution context.

### Partitioning

The table uses **monthly range partitioning** on `block_timestamp`:

- Partitions are named `silver_dip4_transfers_YYYY_MM`
- The migration pre-creates partitions from Dec 2024 through Dec 2026
- **Auto-creation**: if an insert fails because no partition exists for the timestamp, the indexer automatically creates the missing partition and retries. No cron jobs or external tools needed.

### Indexes

| Index | Columns | Purpose |
|-------|---------|---------|
| `silver_dip4_transfers_pk` (unique) | `block_timestamp, block_height, related_receipt_id, event, COALESCE(old_owner_id), COALESCE(new_owner_id), COALESCE(token_id)` | Deduplication via `ON CONFLICT DO NOTHING` |
| `idx_silver_dip4_transfers_old_owner` | `old_owner_id, execution_status, block_timestamp DESC, related_receipt_id DESC` | Sender history queries with cursor pagination |
| `idx_silver_dip4_transfers_new_owner` | `new_owner_id, execution_status, block_timestamp DESC, related_receipt_id DESC` (where memo IS NOT NULL) | Receiver history queries (filtered to rows with memo) |
| `idx_silver_dip4_transfers_receipt` | `related_receipt_id` | Receipt lookups |

### Cross-validation

Compare local PostgreSQL data against ClickHouse production:

```bash
./scripts/cross-validate-pg.sh \
  --pg-url "postgresql://user:pass@localhost:5433/indexer" \
  --block-start 186929800 \
  --block-end 186929900
```

**Tip**: index ~20 extra blocks before the validation range to warm up the Redis receipt-to-tx cache. Otherwise early blocks will have null `tx_hash` on events.
