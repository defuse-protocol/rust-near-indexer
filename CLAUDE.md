# NEAR Defuse Custom Indexer

## Project Overview
Cargo workspace: `indexer-common` (shared library) + `indexer-clickhouse` (binary).
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
- `indexer-common` — all logic: config, database, handlers, cache, metrics, types
- `indexer-clickhouse` — thin binary entry point

## Schema
Source of truth for ClickHouse schema: `clickhouse/init/*.sql`
- `01-core-tables.sql` — events, transactions, receipts, execution_outcomes
- `02-silver-tables.sql` — silver layer tables + materialized views
- `03-gold-views.sql` — gold_view_intents_metrics

## Accounts of Interest — CRITICAL

Three accounts are tracked: `intents.near`, `defuse-alpha.near`, `staging-intents.near`.

**Production vs staging split in materialized views:**
- **Production** (`defuse-alpha.near`, `intents.near`): All `silver_*` tables (without `staging_` prefix) filter for ONLY these two accounts. Their MVs use `contract_id IN ('defuse-alpha.near', 'intents.near')`.
- **Staging** (`staging-intents.near`): Has its own separate `staging_silver_*` tables + MVs that filter for `contract_id = 'staging-intents.near'`.

**DO NOT** mix these. Never add `staging-intents.near` to a production `silver_*` MV filter, and never add production accounts to `staging_silver_*` MVs. Getting this wrong corrupts production analytics data.

## Conventions
- No tests exist yet
- Env config via clap (see `indexer-common/src/config.rs`)
- ClickHouse inserts use exponential backoff (10 retries)
- Redis two-tier cache: main + potential, with TTL
