# Production migration: indexer NULL tx_hash + receipt-cache simplification

**Branch:** `fix/missing-events`
**Date executed against `near_intents_db`:** 2026-05-08

## Context

The indexer used to silently drop events / receipts / execution_outcomes
when the receipt cache could not resolve the parent transaction hash.
Cause: BlocksAPI's server-side filter (a constraint of nearcore /
`near-indexer-framework`, not BlocksAPI itself) doesn't deliver the
originating transaction for chains that begin on accounts outside
`accounts_of_interest`. The receipt cache therefore has no breadcrumb
back to the originating tx, and the leaf receipt arriving at a tracked
contract was being dropped as "unresolvable". A bridge BTC deposit at
block 196366379 was the canonical victim.

This branch:

- Stops dropping rows on cache miss; writes them with NULL tx_hash /
  NULL parent_transaction_hash instead.
- Makes `events.tx_hash` (already nullable), `receipts.parent_transaction_hash`
  and `execution_outcomes.parent_transaction_hash` Nullable end-to-end.
- Adds `coalesce(tx_hash, '')` to the three silver MVs that flow events
  into a non-nullable silver `tx_hash` column.
- Collapses the receipts cache from two keyspaces (main + potential) to
  one — simpler, equivalent retention.

`clickhouse/init/01-core-tables.sql` and `02-silver-tables.sql` reflect
the post-migration schema, but `CREATE TABLE IF NOT EXISTS` is a no-op
against an existing cluster. Operators with persistent ClickHouse
deployments must run the ALTERs and MV swaps below before deploying the
new indexer build, otherwise the indexer's nullable serialization will
mismatch the still-non-nullable column shape and inserts will fail with
`CANNOT_READ_ALL_DATA`.

## Order of operations

The schema migration is **forward-incompatible with the old indexer
binary** (post-ALTER, the old indexer's non-nullable serialization is
rejected). It is **forward-compatible going from old → new**, in this
order:

1. Silver MVs first (compatible with both old and new indexer — the new
   MV body just adds `coalesce(tx_hash, '')` which is a no-op on
   non-NULL inputs).
2. ALTER `receipts.parent_transaction_hash` and
   `execution_outcomes.parent_transaction_hash` to `Nullable(String)`.
3. **Stop the old indexer**, deploy the new build, start. The window
   between step 2 and step 3 is when the old binary will fail; minimise
   it (deploy script ready before kicking off step 2, or do step 2
   immediately before deploy).

If you want zero downtime: keep one events-only indexer running through
the window (it doesn't write to receipts / execution_outcomes, so the
schema change can't break it), and migrate the full-mode indexer only.

## Procedure

### Step 1 — recreate the three silver MVs with the new body

The MVs being recreated all read `events.tx_hash` and write to a silver
column declared `tx_hash String` (non-nullable). The new body wraps with
`coalesce(tx_hash, '')` so any NULL flowing from `events` lands as an
empty string in silver. Keep `silver_X` (the destination tables)
untouched — only the MV definitions change.

For each of the three MVs (`mv_silver_nep_245_events`,
`mv_silver_dip4_transfer`, `mv_staging_silver_dip4_transfer`):

1. Capture `pre = max(events.block_height)`.
2. `DROP TABLE IF EXISTS mv_silver_…`.
3. Recreate with the new body — copy the `CREATE MATERIALIZED VIEW`
   block from `clickhouse/init/02-silver-tables.sql` and run it. The
   only meaningful diff vs the previous body is `coalesce(tx_hash, '')
   AS tx_hash` in the final `SELECT`.
4. Capture `post = max(events.block_height)`.
5. Gap-patch the drop→recreate window:
   ```sql
   INSERT INTO silver_X
   <body of the MV>
   AND block_height >  <pre>
   AND block_height <= <post>;
   ```
   `ReplacingMergeTree` handles any overlap with what the new MV
   already caught after recreation.

ClickHouse Cloud's web SQL console executes only the first statement of
a multi-statement batch (silently — it shows "DROPPED succeeded" even if
the rest is ignored). Use `clickhouse-client --multiquery` reading from
a file, a JDBC client with execute-all, or run statements one at a
time in the web UI.

### Step 2 — ALTER core tables

`receipts` and `execution_outcomes` both have a bloom-filter index on
`parent_transaction_hash`, which blocks `MODIFY COLUMN` on the column
unless it's dropped first. Per table:

```sql
ALTER TABLE receipts DROP INDEX parent_tx_hash_bloom_idx;
ALTER TABLE receipts MODIFY COLUMN parent_transaction_hash Nullable(String);
ALTER TABLE receipts ADD INDEX parent_tx_hash_bloom_idx parent_transaction_hash TYPE bloom_filter() GRANULARITY 1;
ALTER TABLE receipts MATERIALIZE INDEX parent_tx_hash_bloom_idx;
```

```sql
ALTER TABLE execution_outcomes DROP INDEX parent_tx_hash_bloom_idx;
ALTER TABLE execution_outcomes MODIFY COLUMN parent_transaction_hash Nullable(String);
ALTER TABLE execution_outcomes ADD INDEX parent_tx_hash_bloom_idx parent_transaction_hash TYPE bloom_filter() GRANULARITY 1;
ALTER TABLE execution_outcomes MATERIALIZE INDEX parent_tx_hash_bloom_idx;
```

The `MODIFY COLUMN` and `MATERIALIZE INDEX` are background mutations.
Wait until they're all `is_done = 1`:

```sql
SELECT table, mutation_id, command, is_done, parts_to_do, latest_failed_part, latest_fail_reason
FROM system.mutations
WHERE database = currentDatabase()
  AND table IN ('receipts', 'execution_outcomes')
  AND create_time > now() - INTERVAL 1 DAY
ORDER BY create_time DESC;
```

Verify column types post-mutation:

```sql
SELECT name, type FROM system.columns
WHERE database = currentDatabase()
  AND table IN ('receipts', 'execution_outcomes')
  AND name = 'parent_transaction_hash';
```

Expected: both `Nullable(String)`.

If your ClickHouse version supports it, the equivalent one-liner skips
the index dance:

```sql
ALTER TABLE receipts MODIFY COLUMN parent_transaction_hash Nullable(String)
SETTINGS alter_column_secondary_index_mode = 'rebuild';
```

(Tested on CH 26.x; falls back to the explicit DROP/MODIFY/ADD path on
versions that don't recognise the setting.)

### Step 3 — deploy the new indexer build

Stop the running binary, drop the new one in place, start. Watch
`journalctl` (or equivalent) for `Block: NNNNN` lines and absence of
`CANNOT_READ_ALL_DATA`. The new metric `rows_with_null_tx_hash_total`
should start incrementing on bridge-style traffic.

If you have multiple indexer hosts: deploy to all of them. The
old-binary serialization will fail any host still running it after the
ALTERs, even ones intended to be events-only — confirm via the
process's running environ (`/proc/<pid>/environ`) rather than the
systemd unit file, since environ is fixed at process start.

## Rollback

The forward direction has no `_pre_migration` snapshot. To roll back:

1. Stop the new indexer.
2. Reverse the ALTER sequence (drop bloom index, MODIFY COLUMN back to
   `String`, re-add index, MATERIALIZE INDEX). Note: rows currently
   stored as NULL would either need to be deleted or filled with empty
   strings before the `String` MODIFY succeeds; concretely:
   ```sql
   -- Replace NULLs in-place before reverting nullability.
   ALTER TABLE receipts UPDATE parent_transaction_hash = '' WHERE parent_transaction_hash IS NULL;
   ALTER TABLE execution_outcomes UPDATE parent_transaction_hash = '' WHERE parent_transaction_hash IS NULL;
   -- Then the type-revert chain.
   ```
3. Drop and recreate the three silver MVs without the `coalesce`
   wrapper (per the prior `02-silver-tables.sql` shape on `main`).
4. Deploy the previous indexer binary.

In practice rollback is unattractive because it discards real receipts
that were captured for the first time post-migration. Better to fix
forward.

## Verification

After step 3:

1. `events.tx_hash IS NULL` counts should start non-zero and grow:
   ```sql
   SELECT count() FROM events WHERE tx_hash IS NULL AND block_timestamp > now() - INTERVAL 1 HOUR;
   ```
2. `receipts.parent_transaction_hash IS NULL` and
   `execution_outcomes.parent_transaction_hash IS NULL` should also be
   non-zero on bridge-flow traffic.
3. The new metric on each indexer host:
   ```bash
   curl -s http://<host>:8080/metrics | grep rows_with_null_tx_hash_total
   ```
   Expect non-zero on `events`, possibly also on `receipts` and
   `execution_outcomes`.
4. Indexer is keeping pace with NEAR tip:
   ```sql
   SELECT max(block_height) AS latest, max(block_timestamp) AS latest_ts FROM events;
   ```
   Should be advancing; `now() - latest_ts` should stabilise at a small,
   bounded lag.

## Out of scope

- Re-indexing past block ranges to recover events that the old binary
  dropped. Mechanically straightforward (re-run the indexer with
  `--force-from-block-height` over the affected range; ReplacingMergeTree
  dedups against current rows) but consumer-coordinated since their
  dashboards will see one-time row-count bumps.
- RPC fallback in `find_parent_tx_hash` to eliminate NULL tx_hashes
  entirely. Larger change with an external dependency. Tracked
  separately.
