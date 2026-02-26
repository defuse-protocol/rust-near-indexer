# NEAR Defuse Custom Indexer

## Project Overview
Cargo workspace: `indexer-primitives` (types) + `indexer-common` (shared logic) + `indexer-clickhouse` (binary).
Streams NEAR blocks via BlocksAPI, processes transactions/receipts/outcomes/events, stores in ClickHouse. Redis for receipt-to-transaction cache.

## Build & Run

```bash
# Local infra (ClickHouse + Redis with schema auto-created)
docker compose up -d --wait

# Build
cargo build --release

# Run (needs BLOCKSAPI_SERVER_ADDR + BLOCKSAPI_TOKEN in .env)
cp .env.example .env  # then edit
cargo run --release

# Reset local data
docker compose down -v
```

## Key Crates
- `indexer-primitives` — row structs (`EventRow`, `TransactionRow`, `ReceiptRow`, `ExecutionOutcomeRow`), supporting types (`Action`, `ReceiptOrDataId`, `EventJson`). Optional `clickhouse` feature adds `#[derive(Row)]`.
- `indexer-common` — shared logic: config, extractors (parse blocks into row structs), cache (Redis), metrics. **No ClickHouse dependency.**
- `indexer-clickhouse` — binary: ClickHouse client, insert logic (with exponential backoff), handlers that call extractors then insert. Activates `clickhouse` feature on `indexer-primitives`.

### Dependency graph
```
indexer-primitives  (serde, blocksapi; optional clickhouse)
       ↑
indexer-common      (extractors, cache, config, metrics — NO clickhouse)
       ↑
indexer-clickhouse  (handlers, database inserts, ClickHouse client)
```

## Schema
Source of truth for ClickHouse schema: `clickhouse/init/*.sql`
- `01-core-tables.sql` — events, transactions, receipts, execution_outcomes
- `02-silver-tables.sql` — silver layer tables + materialized views + staging tables + unified views
- `03-gold-views.sql` — gold_view_intents_metrics

### Silver tables (production)
- `silver_nep_245_events` — NEP-245 multi-token events (mt_transfer, mt_mint, mt_burn)
- `silver_dip4_token_diff` — DIP-4 token diffs per account per intent
- `silver_dip4_public_keys` — DIP-4 public key add/remove events
- `silver_dip4_intents_executed` — DIP-4 intent execution events
- `silver_dip4_fee_changed` — DIP-4 fee change events
- `silver_dip4_transfer` — DIP-4 transfer events (account_id→receiver_id with tokens map)
- `silver_transfers` (VIEW) — unified view over `silver_nep_245_events` + `silver_dip4_transfer`

### Staging tables
- `staging_silver_dip4_transfer` — mirrors `silver_dip4_transfer` for `staging-intents.near`
- `staging_silver_transfers` (VIEW) — mirrors `silver_transfers` for staging

## Accounts of Interest — CRITICAL

Three accounts are tracked: `intents.near`, `defuse-alpha.near`, `staging-intents.near`.

**Production vs staging split in materialized views:**
- **Production** (`defuse-alpha.near`, `intents.near`): All `silver_dip4_*` tables filter for ONLY these two accounts. Their MVs use `contract_id IN ('defuse-alpha.near', 'intents.near')`. Exception: `silver_nep_245_events` captures all `nep245` events (any contract); production filtering happens in the `silver_transfers` view.
- **Staging** (`staging-intents.near`): Has its own separate `staging_silver_*` tables + MVs that filter for `contract_id = 'staging-intents.near'`.

**DO NOT** mix these. Never add `staging-intents.near` to a production `silver_*` MV filter, and never add production accounts to `staging_silver_*` MVs. Getting this wrong corrupts production analytics data.

## Validation Scripts
- `scripts/validate.sh` — single-instance integrity checks (completeness, referential integrity, account filtering, JSON validity)
- `scripts/cross-validate.sh` — compares local vs production ClickHouse for a block range using aggregate fingerprints (`count()` + `groupBitXor(cityHash64(...))`) on all core + silver tables. Drills down on mismatches.
- Both use `curl` against ClickHouse HTTP interface with CLI flags for connection params.

## Conventions
- No tests exist yet
- Env config via clap (see `indexer-common/src/config.rs`)
- ClickHouse inserts use exponential backoff (10 retries) — lives in `indexer-clickhouse/src/database.rs`
- Redis two-tier cache: main + potential, with TTL
- Extraction logic (parsing blocks into row structs) lives in `indexer-common/src/extractors/`
- Handler logic (calling extractors + inserting into DB) lives in `indexer-clickhouse/src/handlers/`
