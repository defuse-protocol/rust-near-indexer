-- Core tables the indexer directly inserts into.
-- Executed on first ClickHouse container start via /docker-entrypoint-initdb.d/.

CREATE TABLE IF NOT EXISTS events (
    block_height                UInt64 COMMENT 'The height of the block',
    block_timestamp             DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
    block_hash                  String COMMENT 'The hash of the block',
    contract_id                 String COMMENT 'The ID of the account on which the execution outcome happens',
    execution_status            String COMMENT 'The execution outcome status',
    version                     String COMMENT 'The event version',
    standard                    String COMMENT 'The event standard',
    index_in_log                UInt64 COMMENT 'The index in the event log',
    event                       String COMMENT 'The event type',
    data                        String COMMENT 'The event JSON data',
    related_receipt_id          String COMMENT 'The execution outcome receipt ID',
    related_receipt_receiver_id String COMMENT 'The destination account ID',
    related_receipt_predecessor_id String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
    tx_hash                     Nullable(String) COMMENT 'The transaction hash',
    receipt_index_in_block      UInt64 COMMENT 'Index of the receipt within the block',
    INDEX block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
    INDEX related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
    INDEX related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, related_receipt_id, index_in_log)
ORDER BY (block_height, related_receipt_id, index_in_log)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS transactions (
    block_height         UInt64 COMMENT 'The height of the block',
    block_timestamp      DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
    block_hash           String COMMENT 'The hash of the block',
    transaction_hash     String COMMENT 'The transaction hash',
    signer_id            String COMMENT 'The signer account ID',
    receiver_id          String COMMENT 'The receiver account ID',
    actions              String COMMENT 'JSON array of actions',
    INDEX block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX transaction_hash_bloom_idx transaction_hash TYPE bloom_filter() GRANULARITY 1,
    INDEX signer_id_bloom_idx signer_id TYPE bloom_filter() GRANULARITY 1,
    INDEX receiver_id_bloom_idx receiver_id TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, transaction_hash)
ORDER BY (block_height, transaction_hash)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS receipts (
    block_height              UInt64 COMMENT 'The height of the block',
    block_timestamp           DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
    block_hash                String COMMENT 'The hash of the block',
    parent_transaction_hash   String COMMENT 'The parent transaction hash',
    receipt_id                String COMMENT 'The receipt ID',
    receiver_id               String COMMENT 'The receiver account ID',
    predecessor_id            String COMMENT 'The predecessor account ID',
    actions                   String COMMENT 'JSON array of actions',
    INDEX block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX receipt_id_bloom_idx receipt_id TYPE bloom_filter() GRANULARITY 1,
    INDEX receiver_id_bloom_idx receiver_id TYPE bloom_filter() GRANULARITY 1,
    INDEX predecessor_id_bloom_idx predecessor_id TYPE bloom_filter() GRANULARITY 1,
    INDEX parent_tx_hash_bloom_idx parent_transaction_hash TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, receipt_id)
ORDER BY (block_height, receipt_id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS execution_outcomes (
    block_height              UInt64 COMMENT 'The height of the block',
    block_timestamp           DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
    block_hash                String COMMENT 'The hash of the block',
    parent_transaction_hash   String COMMENT 'The parent transaction hash',
    executor_id               String COMMENT 'The executor account ID',
    receipt_ids               Array(String) COMMENT 'Array of receipt IDs',
    status                    String COMMENT 'The execution status',
    logs                      String COMMENT 'JSON array of logs',
    tokens_burnt              String COMMENT 'Tokens burnt (raw string)',
    gas_burnt                 UInt64 COMMENT 'Gas burnt',
    execution_outcome_id      String COMMENT 'The execution outcome ID',
    INDEX block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX executor_id_bloom_idx executor_id TYPE bloom_filter() GRANULARITY 1,
    INDEX parent_tx_hash_bloom_idx parent_transaction_hash TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, execution_outcome_id)
ORDER BY (block_height, execution_outcome_id)
SETTINGS index_granularity = 8192;
