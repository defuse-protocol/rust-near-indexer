# Silver-layer UInt128 / Int256 / event-log provenance migration

**Branch:** `fix/clickhouse-schema-data-types`
**Executed:** 2026-05-07
**Status:** completed against prod (`near_intents_db`); soak / `*_pre_migration` cleanup pending

## Context

Downstream consumer reported off-by-many numbers in their balance-deltas. Root
cause: silver-layer `amount` columns were `Float64`, which silently drops
precision past 2^53 on raw u128 token amounts and signed diffs.

Fix bumped the affected columns to `UInt128` / `Int256` and added per-log
provenance to `silver_nep_245_events` so legitimate per-log duplicates aren't
deduped away.

| Table                              | Column(s)                                     | Old        | New                  |
|------------------------------------|-----------------------------------------------|------------|----------------------|
| `silver_nep_245_events`            | `amount`                                      | `Nullable(Float64)` | `Nullable(UInt128)` |
| `silver_nep_245_events`            | `index_in_log`, `receipt_index_in_block`      | (absent)   | `UInt64` (added)     |
| `silver_nep_245_events`            | ORDER BY                                      | …          | now includes `index_in_log` (prevents ReplacingMergeTree dedup of legitimate per-log duplicates) |
| `silver_dip4_transfer`             | `amount`                                      | `Nullable(Float64)` | `Nullable(UInt128)` |
| `staging_silver_dip4_transfer`     | `amount`                                      | `Nullable(Float64)` | `Nullable(UInt128)` |
| `silver_dip4_token_diff`           | `diff_positive_amount`, `diff_negative_amount` | `Float64`  | `Int256`             |
| `silver_transfers` (view)          | `amount`                                      | `Nullable(Float64)` | `Nullable(UInt128)` |
| `staging_silver_transfers` (view)  | `amount`                                      | `Nullable(Float64)` | `Nullable(UInt128)` |

## What we actually did (shadow-and-swap, no indexer pause)

The original draft of this doc proposed a drop-and-rebuild that would have
required pausing the indexer. We instead used the shadow-and-swap variant
because the indexer had to keep ingesting `events` throughout. ClickHouse
can't `ALTER MODIFY Float64 → UInt128` on a populated MergeTree, so we
side-stepped it.

### Sequence (executed against `near_intents_db` on ClickHouse Cloud)

1. **Shadow tables.** Created `silver_X_v2` with new schema (engine
   `SharedReplacingMergeTree`, mirroring prod's structural choices).
2. **Shadow MVs.** Created `mv_silver_X_v2 TO silver_X_v2` with new bodies
   (UInt128/Int256 casts). From this moment forward, every new `events`
   insert wrote to **both** old and new silvers in parallel.
3. **Captured `cutoff_block = 197259711`** at 2026-05-07 08:18:55 UTC.
4. **Backfilled** `silver_X_v2` for `block_height <= cutoff_block` from
   `events` (4 INSERTs, full table per table, no chunking needed in prod).
5. **Patched the historical pre-filter gap** for `nep_245` and `token_diff`
   (see Option A below).
6. **Verified** row counts, block-height ranges, and Float64 precision-loss
   surface side-by-side. Data analyst signed off.
7. **Cutover (one batch):**
   - Dropped old MVs, shadow MVs, and unified views.
   - `EXCHANGE TABLES silver_X AND silver_X_v2` × 4 (atomic per pair).
   - `RENAME TABLE silver_X_v2 TO silver_X_pre_migration` × 4.
   - Recreated MVs with no-filter bodies (Option A, see below).
   - Recreated unified views `silver_transfers`, `staging_silver_transfers`.
8. **Captured `post_swap_cutoff = 197279330`**, then **gap-patched**
   `block_height BETWEEN 197271612 AND 197279330` into each new silver from
   `events`. ReplacingMergeTree handled overlap with what the new MVs had
   already caught.

Total no-write window during the cutover: a few seconds (the gap patch
covers it deterministically). Indexer was never paused; downstream consumers
saw no missing data, only the brief moment when the unified views were
absent between drop and recreate.

## Pre-cutover discoveries (documented for future migrations)

These came up during the side-by-side validation and influenced the final
plan:

1. **`silver_dip4_token_diff_new` and `silver_dip4_token_diff_rk` are
   downstream consumer tables**, not abandoned migrations as the names
   suggested. They source from `events` directly, have a different schema
   (`token_in`/`amount_in`/`token_out`/`amount_out` split, plus
   `block_height_negative` for the `_rk` variant). They have the same
   `Float64` precision bug — out of scope here, the consumer needs their own
   fix. We renamed our shadow tables to `_v2` to avoid colliding with
   `_new`.

2. **The old `mv_staging_silver_dip4_transfer` body had a stale
   `block_timestamp >= '2025-12-01 00:00:00'` filter** that silently dropped
   ~396 historical staging-intents.near transfers. Backfill caught them.
   Post-cutover, prod has those rows for the first time. Worth flagging to
   anyone who relied on staging silver for analytics.

3. **`silver_nep_245_events` and `silver_dip4_token_diff` had stale
   `block_timestamp >=` filters** in their MV bodies that excluded ~60k +
   ~24k legitimate pre-Feb-2025 rows from `events`. The old silvers had
   those rows because the filters were *added later* — old data captured
   before the filter was added remained, but a fresh backfill respecting
   the current filter would not reproduce it. We discovered this when v2
   counts initially trailed old by ~0.1% and `min(block_height)` differed.
   Resolution: **Option A — drop the filters**. See below.

4. **`silver_dip4_token_diff_pre_migration` had the same Float64 bug
   inflated by ReplacingMergeTree dupes from months of live MV churn**, but
   `FINAL` showed the gap was minimal (~0.04%); the bulk of the gap was
   actually item 3.

5. **ClickHouse Cloud's web SQL console only executes the first statement
   of a multi-statement batch** (silently — it shows "DROPPED succeeded"
   even if the rest of the batch was ignored). For the cutover we used
   `clickhouse-client --multiquery` reading the whole file, which submits
   it as one request. Don't run cutover batches via the web UI.

6. **Replica lag on `Shared*MergeTree` is real.** The first count against
   the unified views right after the swap returned `0`; a re-run a moment
   later returned the correct count. Not a bug in the views — async
   replication catching up. Worth noting for any "did the cutover work?"
   sanity-check timing.

## Option A — drop the timestamp filters

The `block_timestamp >= …` filters in the old MV bodies were tightened
after the silvers had already captured pre-filter data. Three options were
considered (drop filter / port early rows from old silver / accept the
loss). We picked **A — drop the filters**, which:

- Matches the unfiltered behavior of `silver_dip4_transfer` (which had no
  filter and showed exact parity).
- Catches all historical events that exist in `events` going back to
  October 2024, including 60k nep245 events and 24k token_diff events that
  were silently missing before.
- Forward operation is unaffected — all new events have timestamps far past
  the previous cutoff, so the filter was a no-op for forward data anyway.

`clickhouse/init/02-silver-tables.sql` reflects this: the
`mv_silver_nep_245_events` and `mv_silver_dip4_token_diff` bodies no longer
filter on `block_timestamp`.

## Headline numbers (for the consumer)

- **`silver_nep_245_events`: 4,105 rows had silently corrupted Float64
  amounts.** These are the rows where
  `toFloat64(new.amount) != old.amount` joining new vs old on the natural
  key. The downstream balance-deltas consumer was reading these wrong.
- **`staging_silver_dip4_transfer`: +396 rows** post-migration that were
  silently dropped by the old MV's bad filter.
- Equivalent corruption count for `silver_dip4_transfer` and
  `silver_dip4_token_diff` was not produced — the validation queries OOM'd
  on prod-sized data. Re-run if the consumer needs the exact number.

Concrete examples of the corruption fixed (pulled from the spot-check
during cutover):

| `block_height` | corrupted Float64 (old) | correct UInt128 (new) | delta (base units) |
|---|---|---|---|
| 197269865 | 11,012,270,970,835,990,000 | 11,012,270,970,835,991,558 | 1,558 |
| 197252041 | 146,169,009,813,987,980,000 | 146,169,009,813,987,991,571 | 11,571 |
| 197242489 | 1.4913373305509418e24 | 1,491,337,330,550,941,889,763,779 | unrenderable in Float64 |

## Consumer-side breaking changes

The new `UInt128` / `Int256` columns no longer round-trip through
`Float64`. Anything in consumer queries that did so will silently keep
producing the old, corrupted answer:

- **`printf('%.0f', amount)` is now wrong** — it casts to `Float64` first,
  re-introducing the same precision loss this migration fixed. Use
  **`toString(amount)`**; ClickHouse renders the full integer to its exact
  decimal representation directly. For `Nullable(UInt128)`, wrap with
  `coalesce(toString(amount), '')` if the consumer wants empty string for
  NULLs.
- **Multiplying by floats** (e.g. `amount * 1e-24` for human units) forces
  a `Float64` cast and is lossy. Use integer division
  (`amount / pow(10, 24)`) and only convert to a float at the very last
  step, for visual rendering.
- **`AVG`, `STDDEV`, etc.** still return `Float64` internally; the
  underlying data is correct, but aggregates lose precision. Sums stay in
  the wide integer type and are exact.

## Rollback (still available during soak)

The four `silver_X_pre_migration` tables hold the original Float64 data.
Rollback is symmetric — drop new MVs and views, `EXCHANGE TABLES` back,
recreate old MVs from the snapshot of the old DDL. We'd lose a few seconds
of forward writes (recoverable from `events`) but no historical data.

## Cleanup (after soak — 24-48 h post-cutover, consumer DAG re-run)

```sql
DROP TABLE silver_nep_245_events_pre_migration;
DROP TABLE silver_dip4_transfer_pre_migration;
DROP TABLE staging_silver_dip4_transfer_pre_migration;
DROP TABLE silver_dip4_token_diff_pre_migration;
```

## Out of scope (status as of 2026-05-07)

- ~~`staging_silver_dip4_token_diff` (+ MV)~~ — **done same day** via the
  simpler drop-and-recreate path (low staging traffic + a few minutes of
  empty table is acceptable; data is regeneratable from `events`).
  Sequence: `DROP TABLE mv_staging_silver_dip4_token_diff` → `DROP TABLE
  staging_silver_dip4_token_diff` → recreate with the new schema (Int256,
  no `block_timestamp` filter) → recreate the MV with the new body →
  `INSERT INTO … SELECT … FROM events WHERE block_height <= cutoff` to
  backfill. ReplacingMergeTree dedups any overlap between the live MV
  catches and the backfill. End state: 6,527 rows in
  `staging_silver_dip4_token_diff`, min block_height ≈ 151953380 (mid-2025)
  → vs the old filtered MV which only saw post-Dec-2025 events.
- ~~`silver_dip4_token_diff_rk`~~ — **dropped same day**. Provenance was
  unclear (created directly on prod, never in repo, not the analyst's).
  The `block_height_negative` PK trick is for fast `ORDER BY block_height
  DESC` scans, but no consumer query was depending on it.
- `silver_dip4_token_diff_new` — independent consumer-owned table (the
  data analyst's), same Float64 bug, analyst needs to migrate their own
  schema.
- `silver_dip4_mt_withdraw` — prod-only, not in repo init. Audit
  separately for any precision-sensitive columns.

## Repo parity with prod (also captured on this branch)

Prod had four `staging_silver_dip4_*` tables (+ their MVs) that were
hand-extended onto prod and never landed in repo init. They are now in
`clickhouse/init/02-silver-tables.sql` so a fresh `docker compose up
-d --wait` brings up the same shape prod has:

- `staging_silver_dip4_token_diff` (+ `mv_*`) — repo schema is the
  **post-fix** version (`Int256`, no `block_timestamp` filter);
  prod migration is the deferred item above.
- `staging_silver_dip4_public_keys` (+ `mv_*`) — mirrors prod literally
  (no precision concern, keeps the `block_timestamp >= '2025-12-01'`
  filter).
- `staging_silver_dip4_intents_executed` (+ `mv_*`) — mirrors prod
  literally.
- `staging_silver_dip4_fee_changed` (+ `mv_*`) — mirrors prod literally.

## Lessons / runbook for future ClickHouse silver migrations

1. **Always `DESCRIBE` prod first.** Repo init can drift from prod
   (different engines on Cloud, hand-added staging tables, downstream
   shadow tables you don't own). The whole `_v2` naming, the staging
   filter discovery, and the consumer-table flagging came from
   `system.tables` and `system.columns` queries before we wrote any DDL.
2. **Shadow-and-swap beats drop-and-rebuild whenever the indexer needs to
   keep running.** The dual-write window during shadow operation gives you
   side-by-side validation with no data loss risk, and `EXCHANGE TABLES`
   makes the swap atomic per pair.
3. **Capture `pre_swap` and `post_swap` block heights and gap-patch.**
   The few-second window between dropping old MVs and recreating new ones
   is the only place data could be missed; the gap patch over `events` is
   deterministic and ReplacingMergeTree dedups any overlap.
4. **Don't run the cutover batch via the CH Cloud web UI.** It silently
   only fires the first statement. Use `clickhouse-client --multiquery
   < cutover.sql` or a JDBC client with "execute all".
5. **Expect replica lag on `Shared*MergeTree`.** Re-run a count after
   30s before declaring the cutover broken.
