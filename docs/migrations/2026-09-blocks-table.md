# Production migration: `blocks` table

**Branch:** `feat/blocks-table`

## Context

The ClickHouse indexer now writes one row per block (`block_height`,
`block_timestamp`, `block_hash`) into `blocks`, keyed by height, so a block can
be found cheaply by height (primary key), timestamp (minmax) or hash (bloom). No backfill: the table starts at whatever block
the new build indexes first.

## Must run BEFORE deploying the new binary

`CREATE TABLE IF NOT EXISTS` in `01-core-tables.sql` only runs on a fresh
ClickHouse. Against prod the table must be created by hand — otherwise every
block insert fails, retries 10×, and the indexer exits.

```sql
CREATE TABLE IF NOT EXISTS blocks (
    block_height         UInt64 COMMENT 'The height of the block',
    block_timestamp      DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
    block_hash           String COMMENT 'The hash of the block',
    INDEX block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_hash_bloom_idx block_hash TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height)
ORDER BY (block_height)
SETTINGS index_granularity = 8192;
```

## Querying

```sql
-- Last block at or before T
SELECT * FROM blocks
WHERE block_timestamp <= toDateTime64('2026-10-01 12:00:00', 9, 'UTC')
ORDER BY block_timestamp DESC
LIMIT 1;
```

Earliest covered time: `SELECT min(block_timestamp) FROM blocks`.
