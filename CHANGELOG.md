# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Fixed

- **Indexer no longer drops events / receipts / execution_outcomes when the
  parent transaction hash cannot be resolved from the receipt cache.** A
  bridge deposit reaches a tracked contract (e.g. `intents.near`) via
  intermediaries on accounts not in `accounts_of_interest` (e.g.
  `bridge-mng.near` → `btc.omft.near` → `intents.near`). BlocksAPI doesn't
  deliver the originating tx in that case, so the receipt cache has no
  parent-tx mapping for the leaf receipt — and the row was silently
  dropped at `indexer-common/src/extractors/events.rs` and
  `indexer-common/src/extractors/receipts_and_outcomes.rs`. Rows now land
  with `tx_hash = NULL` (events) / `parent_transaction_hash = NULL`
  (receipts, execution_outcomes) and are observable via the new
  `rows_with_null_tx_hash_total{row_type=…}` metric. Schema updated:
  `receipts.parent_transaction_hash` and
  `execution_outcomes.parent_transaction_hash` are now `Nullable(String)`
  (events.tx_hash was already nullable). The three silver MVs that flow
  events.tx_hash into a non-nullable silver column
  (`mv_silver_nep_245_events`, `mv_silver_dip4_transfer`,
  `mv_staging_silver_dip4_transfer`) now coalesce NULL → empty string in
  their final SELECT to keep the silver-side `tx_hash String` columns
  intact. Historical events that were already dropped need a separate
  re-index pass to recover.
- **Silver-layer numeric precision** — `silver_nep_245_events.amount`,
  `silver_dip4_transfer.amount`, and `staging_silver_dip4_transfer.amount`
  changed from `Nullable(Float64)` to `Nullable(UInt128)`. The old
  `Float64` columns silently rounded raw u128 token amounts past 2^53.
  Consumer-facing impact: queries that did `printf('%.0f', amount)`
  re-introduced the precision loss; use `toString(amount)` instead. See
  `docs/migrations/2026-05-silver-uint128.md` for the full migration
  story, headline numbers, and the executed cutover sequence.
- **`silver_dip4_token_diff` and `staging_silver_dip4_token_diff` reshaped
  to match the analyst's `silver_dip4_token_diff_new`** — the `(diff_positive_*,
  diff_negative_*)` shape on `Float64` is replaced with the
  `(token_in, amount_in, token_out, amount_out, token_fee, amount_fee)`
  shape on `Int256`, plus new payload columns `tx_hash`, `index_in_log`,
  `idx`, `tokens_cnt`, `receipt_index_in_block`. The old narrow ORDER BY
  `(block_height, related_receipt_id, intent_hash)` was silently
  collapsing ~50% of token_diff rows because the MV emits one row per
  diff token entry but the dedup key wasn't unique-per-entry; new
  ORDER BY `(block_height, related_receipt_id, index_in_log, idx)`
  preserves every entry. Consumer-facing impact: `silver_dip4_token_diff`
  is now an in-place replacement for `silver_dip4_token_diff_new`
  (same shape, but with `Int256` amounts); consumers querying `_new`
  should switch to `silver_dip4_token_diff` and the old
  `_new` table can be dropped after migration.

### Added

- `silver_nep_245_events.index_in_log` and `receipt_index_in_block`
  (`UInt64`). Added to the table's `ORDER BY` so that legitimate per-log
  duplicates are no longer collapsed by `ReplacingMergeTree`.
- `docs/migrations/2026-05-silver-uint128.md` — runbook + post-mortem for
  the silver-layer precision migration, including discovered side-issues
  (stale `block_timestamp` filters in old MV bodies, ClickHouse Cloud web
  UI silently dropping multi-statement batches, replica lag on
  `Shared*MergeTree`).
- Four `staging_silver_dip4_*` tables (+ matching MVs) added to
  `clickhouse/init/02-silver-tables.sql` for parity with prod, which had
  been hand-extended without landing in repo init:
  `staging_silver_dip4_token_diff`, `staging_silver_dip4_public_keys`,
  `staging_silver_dip4_intents_executed`, `staging_silver_dip4_fee_changed`.
  `staging_silver_dip4_token_diff` uses the post-fix schema (`Int256`,
  no `block_timestamp` filter); the corresponding prod-side migration is
  deferred and follows the same shadow-and-swap playbook in
  `docs/migrations/2026-05-silver-uint128.md`.

### Changed

- `mv_silver_nep_245_events` and `mv_silver_dip4_token_diff` bodies in
  `clickhouse/init/02-silver-tables.sql` no longer filter on
  `block_timestamp`. The old filters silently excluded ~60k + ~24k
  legitimate pre-Feb-2025 events from the silvers. Forward operation is
  unaffected (all new events have timestamps far past the old cutoff
  anyway).
