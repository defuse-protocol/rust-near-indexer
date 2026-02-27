# NEAR Defuse Custom Indexer

## Project Overview
Cargo workspace: `indexer-primitives` (types) + `indexer-common` (shared logic) + `indexer-clickhouse` (binary) + `indexer-explorer` (binary).
Streams NEAR blocks via BlocksAPI, processes transactions/receipts/outcomes/events, stores in ClickHouse and/or PostgreSQL. Redis for receipt-to-transaction cache.

## Build & Run

```bash
# Local infra (ClickHouse + Redis + Postgres with schema auto-created)
docker compose up -d --wait

# Build
cargo build --release

# Run ClickHouse indexer (needs BLOCKSAPI_SERVER_ADDR + BLOCKSAPI_TOKEN in .env)
cp .env.example .env  # then edit
cargo run --release --bin near-defuse-indexer

# Run Postgres indexer (needs POSTGRES_URL in addition to BlocksAPI env vars)
cargo run --release --bin indexer-explorer

# Reset local data
docker compose down -v
```

## Key Crates
- `indexer-primitives` — row structs (`EventRow`, `TransactionRow`, `ReceiptRow`, `ExecutionOutcomeRow`), supporting types (`Action`, `ReceiptOrDataId`, `EventJson`). Optional `clickhouse` feature adds `#[derive(Row)]`.
- `indexer-common` — shared logic: config, extractors (parse blocks into row structs), cache (Redis), metrics. **No ClickHouse dependency.**
- `indexer-clickhouse` — binary: ClickHouse client, insert logic (with exponential backoff), handlers that call extractors then insert. Activates `clickhouse` feature on `indexer-primitives`.
- `indexer-explorer` — binary: indexes DIP-4 transfer events into PostgreSQL. Own `AppConfig` in `indexer-explorer/src/config.rs`. Uses `sqlx` with migrations. Monthly-partitioned `silver_dip4_transfers` table with auto-partition creation on insert.

### Dependency graph
```
indexer-primitives  (serde, blocksapi; optional clickhouse)
       ↑
indexer-common      (extractors, cache, config, metrics — NO clickhouse, NO postgres)
       ↑              ↑
indexer-clickhouse    indexer-explorer
(ClickHouse client)   (sqlx/Postgres client)
```

## Schema

### ClickHouse
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

### PostgreSQL
Source of truth for Postgres schema: `indexer-explorer/migrations/`
- `silver_dip4_transfers` — monthly partitioned by `block_timestamp` (`PARTITION BY RANGE`). Migration pre-creates partitions Dec 2024–Dec 2026. Auto-creation on insert: if PG returns "no partition of relation", the indexer creates the missing monthly partition and retries.
- Docker Postgres is on **port 5433** (mapped from container 5432). URL: `postgres://indexer:indexer@localhost:5433/defuse_indexer`.

## Accounts of Interest — CRITICAL

Three accounts are tracked: `intents.near`, `defuse-alpha.near`, `staging-intents.near`.

**Production vs staging split in materialized views:**
- **Production** (`defuse-alpha.near`, `intents.near`): All `silver_dip4_*` tables filter for ONLY these two accounts. Their MVs use `contract_id IN ('defuse-alpha.near', 'intents.near')`. Exception: `silver_nep_245_events` captures all `nep245` events (any contract); production filtering happens in the `silver_transfers` view.
- **Staging** (`staging-intents.near`): Has its own separate `staging_silver_*` tables + MVs that filter for `contract_id = 'staging-intents.near'`.

**DO NOT** mix these. Never add `staging-intents.near` to a production `silver_*` MV filter, and never add production accounts to `staging_silver_*` MVs. Getting this wrong corrupts production analytics data.

## Validation Scripts
- `scripts/validate.sh` — single-instance ClickHouse integrity checks (completeness, referential integrity, account filtering, JSON validity)
- `scripts/cross-validate.sh` — compares local vs production ClickHouse for a block range using aggregate fingerprints (`count()` + `groupBitXor(cityHash64(...))`) on all core + silver tables. Drills down on mismatches.
- `scripts/cross-validate-pg.sh` — compares local Postgres `silver_dip4_transfers` against production ClickHouse `silver_dip4_transfer` for a block range. 4 phases: total row count, per-block counts, row-level content diff (excluding amount), amount comparison with relative tolerance (Float64 vs NUMERIC). Uses `psql` for PG and `curl` for CH.
- ClickHouse scripts use `curl` against the HTTP interface; PG script uses `psql`. All accept connection params via CLI flags and auto-source `.env` from project root.

### How to validate after code changes (indexing a block range)

When verifying that local indexing matches production, follow this approach:

1. **Nuke and recreate local infra** for a clean slate:
   ```bash
   docker compose down -v && docker compose up -d --wait
   ```
2. **Pick a block range** to validate, e.g. `186929800..186929900`. The `.env` has `BLOCK_HEIGHT` and `BLOCK_END` for this.
3. **Index ~20 extra blocks before** the validation range to warm up the Redis receipt-to-tx cache. Without this, early blocks will have null `tx_hash` on events (cache misses). Set `BLOCK_HEIGHT` to ~20 blocks before the validation start (e.g., `186929780` for validating `186929800..186929900`).
4. **Run both indexers** (they can run in parallel since they share Redis but write to different DBs). Use different `--metrics-server-port` values to avoid port conflicts:
   ```bash
   cargo run --release --bin near-defuse-indexer -- --metrics-server-port 8000 &
   cargo run --release --bin indexer-explorer -- --database-url "postgres://indexer:indexer@localhost:5433/defuse_indexer" --metrics-server-port 8001 &
   wait
   ```
5. **Run ClickHouse cross-validation**:
   ```bash
   ./scripts/cross-validate.sh \
     --local-url http://localhost:8123 --local-user indexer --local-password indexer --local-database default \
     --block-start 186929800 --block-end 186929900
   ```
6. **Run Postgres cross-validation**:
   ```bash
   ./scripts/cross-validate-pg.sh \
     --pg-url "postgres://indexer:indexer@localhost:5433/defuse_indexer" \
     --block-start 186929800 --block-end 186929900
   ```
7. Both scripts exit 0 on success, 1 on failure, with detailed diff output on mismatches. Prod CH credentials are auto-sourced from `.env`.

## Code Quality

Before finishing any code change, always:
1. Run `cargo fmt --check` — if it reports issues, run `cargo fmt` to fix them.
2. Run `cargo clippy --workspace` — address all warnings before considering the task done.

## Conventions
- No tests exist yet
- Env config via clap (`indexer-common/src/config.rs` for shared config, `indexer-explorer/src/config.rs` for PG-specific config)
- ClickHouse inserts use exponential backoff (10 retries) — lives in `indexer-clickhouse/src/database.rs`
- Postgres inserts use manual retry loop (10 attempts) with auto-partition creation — lives in `indexer-explorer/src/database.rs`
- Redis two-tier cache: main + potential, with TTL
- Extraction logic (parsing blocks into row structs) lives in `indexer-common/src/extractors/`
- Handler logic (calling extractors + inserting into DB) lives in `indexer-clickhouse/src/handlers/` and `indexer-explorer/src/handlers/`
- Use `try_into()` instead of `as i64` for u64→i64 casts to avoid silent overflow
