-- Silver layer tables and materialized views.
-- Executed on first ClickHouse container start via /docker-entrypoint-initdb.d/.
--
-- Note: The defuse_assets MV is intentionally excluded because it fetches from
-- an external URL (https://api-mng-console.chaindefuser.com) which won't work
-- in local/CI environments.

-- ============================================================================
-- defuse_assets (table only, no MV)
-- ============================================================================

CREATE TABLE IF NOT EXISTS defuse_assets (
    blockchain          String COMMENT 'The blockchain',
    contract_address    String COMMENT 'The contract address',
    decimals            UInt64 COMMENT 'Decimals',
    defuse_asset_id     String COMMENT 'The asset ID',
    price               Float64 COMMENT 'The price',
    price_updated_at    DateTime64(9, 'UTC') COMMENT 'The price updated timestamp',
    symbol              String COMMENT 'The symbol'
) ENGINE = ReplacingMergeTree
PRIMARY KEY (defuse_asset_id, price_updated_at)
ORDER BY (defuse_asset_id, price_updated_at);


-- ============================================================================
-- silver_nep_245_events + MV
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver_nep_245_events (
    block_height                UInt64 COMMENT 'The height of the block',
    block_timestamp             DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
    block_hash                  String COMMENT 'The hash of the block',
    tx_hash                     String COMMENT 'The hash of transaction',
    contract_id                 String COMMENT 'The ID of the account on which the execution outcome happens',
    execution_status            String COMMENT 'The execution outcome status',
    version                     String COMMENT 'The event version',
    standard                    String COMMENT 'The event standard',
    event                       String COMMENT 'The event type',
    related_receipt_id          String COMMENT 'The execution outcome receipt ID',
    related_receipt_receiver_id String COMMENT 'The destination account ID',
    related_receipt_predecessor_id String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
    memo                        Nullable(String) COMMENT 'The event memo',
    old_owner_id                Nullable(String) COMMENT 'The old owner account ID',
    new_owner_id                Nullable(String) COMMENT 'The new owner account ID',
    token_id                    Nullable(String) COMMENT 'The token ID',
    amount                      Nullable(Float64) COMMENT 'The amount',
    INDEX nep_245_block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX nep_245_contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
    INDEX nep_245_related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
    INDEX nep_245_related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, related_receipt_id, event, old_owner_id, new_owner_id, token_id)
ORDER BY (block_height, related_receipt_id, event, old_owner_id, new_owner_id, token_id)
SETTINGS allow_nullable_key = true, index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_silver_nep_245_events TO silver_nep_245_events (
    block_height                UInt64,
    block_timestamp             DateTime64(9, 'UTC'),
    block_hash                  String,
    tx_hash                     String,
    contract_id                 String,
    execution_status            String,
    version                     String,
    standard                    String,
    event                       String,
    related_receipt_id          String,
    related_receipt_receiver_id String,
    related_receipt_predecessor_id String,
    memo                        String,
    old_owner_id                Nullable(String),
    new_owner_id                Nullable(String),
    token_id                    String,
    amount                      Float64
) AS
WITH decoded_events AS (
    SELECT *, arrayJoin(JSONExtractArrayRaw(data)) AS data_row
    FROM events
    WHERE (standard = 'nep245') AND (block_timestamp >= '2025-02-12 22:10:00')
), tokens AS (
    SELECT *, coalesce(JSON_VALUE(data_row, '$.memo'), '') AS memo,
           if(event = 'mt_transfer', JSON_VALUE(data_row, '$.old_owner_id'), JSON_VALUE(data_row, '$.owner_id')) AS old_owner_id,
           if(event = 'mt_transfer', JSON_VALUE(data_row, '$.new_owner_id'), JSON_VALUE(data_row, '$.owner_id')) AS new_owner_id,
           JSONExtractArrayRaw(data_row, 'token_ids') AS token_ids,
           JSONExtractArrayRaw(data_row, 'amounts') AS amounts
    FROM decoded_events
), tokens_flattened AS (
    SELECT *, (arrayJoin(arrayZip(token_ids, amounts)) AS t).1 AS token_id, t.2 AS amount
    FROM tokens
)
SELECT block_height, block_timestamp, block_hash, tx_hash, contract_id, execution_status, version, standard, event, related_receipt_id, related_receipt_receiver_id, related_receipt_predecessor_id, memo, old_owner_id, new_owner_id, replaceAll(token_id, '"', '') AS token_id, CAST(replaceAll(amount, '"', ''), 'Float64') AS amount
FROM tokens_flattened
SETTINGS function_json_value_return_type_allow_nullable = true;


-- ============================================================================
-- silver_dip4_token_diff + MV
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver_dip4_token_diff (
    block_height                UInt64 COMMENT 'The height of the block',
    block_timestamp             DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
    block_hash                  String COMMENT 'The hash of the block',
    contract_id                 String COMMENT 'The ID of the account on which the execution outcome happens',
    execution_status            String COMMENT 'The execution outcome status',
    version                     String COMMENT 'The event version',
    standard                    String COMMENT 'The event standard',
    event                       String COMMENT 'The event type',
    related_receipt_id          String COMMENT 'The execution outcome receipt ID',
    related_receipt_receiver_id String COMMENT 'The destination account ID',
    related_receipt_predecessor_id String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
    account_id                  String COMMENT 'The token differential account ID',
    diff_positive_token         String COMMENT 'The positive token differential',
    diff_positive_amount        Float64 COMMENT 'The positive amount differential',
    diff_negative_token         String COMMENT 'The negative token differential',
    diff_negative_amount        Float64 COMMENT 'The negative amount differential',
    intent_hash                 String COMMENT 'The hash of the intent',
    referral                    Nullable(String) COMMENT 'The referral of the intent',
    INDEX dif4_diff_block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX dif4_diff_contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
    INDEX dif4_diff_related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
    INDEX dif4_diff_related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, related_receipt_id, intent_hash)
ORDER BY (block_height, related_receipt_id, intent_hash)
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_silver_dip4_token_diff TO silver_dip4_token_diff (
    block_height                UInt64,
    block_timestamp             DateTime64(9, 'UTC'),
    block_hash                  String,
    contract_id                 String,
    execution_status            String,
    version                     String,
    standard                    String,
    event                       String,
    related_receipt_id          String,
    related_receipt_predecessor_id String,
    related_receipt_receiver_id String,
    account_id                  String,
    diff_positive_token         String,
    diff_positive_amount        Float64,
    diff_negative_token         String,
    diff_negative_amount        Float64,
    intent_hash                 String,
    referral                    String
) AS
WITH decoded_events AS (
    SELECT *, arrayJoin(JSONExtractArrayRaw(data)) AS data_row
    FROM events
    WHERE (contract_id IN ('defuse-alpha.near', 'intents.near')) AND (standard = 'dip4') AND (event = 'token_diff') AND (block_timestamp >= '2025-02-18 22:55:00')
), parsed_json AS (
    SELECT *, coalesce(JSON_VALUE(data_row, '$.account_id'), '') AS account_id,
           coalesce(JSON_VALUE(data_row, '$.diff'), '') AS diff,
           coalesce(JSON_VALUE(data_row, '$.intent_hash'), '') AS intent_hash,
           coalesce(JSON_VALUE(data_row, '$.referral'), '') AS referral
    FROM decoded_events
), diff_kvs AS (
    SELECT diff, arrayJoin(JSONExtractKeysAndValues(assumeNotNull(diff), 'Float64')) AS diff_kv, *
    FROM parsed_json
)
SELECT block_height, block_timestamp, block_hash, contract_id, execution_status, version, standard, event, related_receipt_id, related_receipt_predecessor_id, related_receipt_receiver_id, account_id,
       if((diff_kv.2) >= 0, diff_kv.1, '') AS diff_positive_token,
       if((diff_kv.2) >= 0, diff_kv.2, 0) AS diff_positive_amount,
       if((diff_kv.2) < 0, diff_kv.1, '') AS diff_negative_token,
       if((diff_kv.2) < 0, diff_kv.2, 0) AS diff_negative_amount,
       intent_hash, referral
FROM diff_kvs
SETTINGS function_json_value_return_type_allow_nullable = true, function_json_value_return_type_allow_complex = true;


-- ============================================================================
-- silver_dip4_public_keys + MV
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver_dip4_public_keys (
    block_height                UInt64 COMMENT 'The height of the block',
    block_timestamp             DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
    block_hash                  String COMMENT 'The hash of the block',
    contract_id                 String COMMENT 'The ID of the account on which the execution outcome happens',
    execution_status            String COMMENT 'The execution outcome status',
    version                     String COMMENT 'The event version',
    standard                    String COMMENT 'The event standard',
    event                       String COMMENT 'The event type',
    related_receipt_id          String COMMENT 'The execution outcome receipt ID',
    related_receipt_receiver_id String COMMENT 'The destination account ID',
    related_receipt_predecessor_id String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
    account_id                  String COMMENT 'The public key account ID',
    public_key                  String COMMENT 'The public key',
    INDEX dip4_public_keys_block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX dip4_public_keys_contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
    INDEX dip4_public_keys_related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
    INDEX dip4_public_keys_related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, related_receipt_id, account_id)
ORDER BY (block_height, related_receipt_id, account_id)
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_silver_dip4_public_keys TO silver_dip4_public_keys (
    block_height                UInt64,
    block_timestamp             DateTime64(9, 'UTC'),
    block_hash                  String,
    contract_id                 String,
    execution_status            String,
    version                     String,
    standard                    String,
    event                       String,
    related_receipt_id          String,
    related_receipt_predecessor_id String,
    related_receipt_receiver_id String,
    account_id                  String,
    public_key                  String
) AS
WITH decoded_events AS (
    SELECT *, data AS data_row
    FROM events
    WHERE (contract_id IN ('defuse-alpha.near', 'intents.near')) AND (standard = 'dip4') AND (event IN ('public_key_added', 'public_key_removed')) AND (block_timestamp >= '2025-02-12 23:35:00')
)
SELECT block_height, block_timestamp, block_hash, contract_id, execution_status, version, standard, event, related_receipt_id, related_receipt_predecessor_id, related_receipt_receiver_id, coalesce(JSON_VALUE(data_row, '$.account_id'), '') AS account_id, coalesce(JSON_VALUE(data_row, '$.public_key'), '') AS public_key
FROM decoded_events
SETTINGS function_json_value_return_type_allow_nullable = true, function_json_value_return_type_allow_complex = true;


-- ============================================================================
-- silver_dip4_intents_executed + MV
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver_dip4_intents_executed (
    block_height                UInt64 COMMENT 'The height of the block',
    block_timestamp             DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
    block_hash                  String COMMENT 'The hash of the block',
    contract_id                 String COMMENT 'The ID of the account on which the execution outcome happens',
    execution_status            String COMMENT 'The execution outcome status',
    version                     String COMMENT 'The event version',
    standard                    String COMMENT 'The event standard',
    event                       String COMMENT 'The event type',
    related_receipt_id          String COMMENT 'The execution outcome receipt ID',
    related_receipt_receiver_id String COMMENT 'The destination account ID',
    related_receipt_predecessor_id String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
    account_id                  String COMMENT 'The intent executed account ID',
    intent_hash                 String COMMENT 'The intent executed hash',
    INDEX dip4_intents_executed_block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX dip4_intents_executed_contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
    INDEX dip4_intents_executed_related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
    INDEX dip4_intents_executed_related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, related_receipt_id, intent_hash)
ORDER BY (block_height, related_receipt_id, intent_hash)
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_silver_dip4_intents_executed TO silver_dip4_intents_executed (
    block_height                UInt64,
    block_timestamp             DateTime64(9, 'UTC'),
    block_hash                  String,
    contract_id                 String,
    execution_status            String,
    version                     String,
    standard                    String,
    event                       String,
    related_receipt_id          String,
    related_receipt_predecessor_id String,
    related_receipt_receiver_id String,
    account_id                  String,
    intent_hash                 String
) AS
WITH decoded_events AS (
    SELECT *, arrayJoin(JSONExtractArrayRaw(data)) AS data_row
    FROM events
    WHERE (contract_id IN ('defuse-alpha.near', 'intents.near')) AND (standard = 'dip4') AND (event = 'intents_executed') AND (block_timestamp >= '2025-02-12 23:45:00')
)
SELECT block_height, block_timestamp, block_hash, contract_id, execution_status, version, standard, event, related_receipt_id, related_receipt_predecessor_id, related_receipt_receiver_id, coalesce(JSON_VALUE(data_row, '$.account_id'), '') AS account_id, coalesce(JSON_VALUE(data_row, '$.intent_hash'), '') AS intent_hash
FROM decoded_events
SETTINGS function_json_value_return_type_allow_nullable = true, function_json_value_return_type_allow_complex = true;


-- ============================================================================
-- silver_dip4_fee_changed + MV
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver_dip4_fee_changed (
    block_height                UInt64 COMMENT 'The height of the block',
    block_timestamp             DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
    block_hash                  String COMMENT 'The hash of the block',
    contract_id                 String COMMENT 'The ID of the account on which the execution outcome happens',
    execution_status            String COMMENT 'The execution outcome status',
    version                     String COMMENT 'The event version',
    standard                    String COMMENT 'The event standard',
    event                       String COMMENT 'The event type',
    related_receipt_id          String COMMENT 'The execution outcome receipt ID',
    related_receipt_receiver_id String COMMENT 'The destination account ID',
    related_receipt_predecessor_id String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
    old_fee                     String COMMENT 'The old fee',
    new_fee                     String COMMENT 'The new fee',
    INDEX dip4_fee_changed_block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX dip4_fee_changed_contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
    INDEX dip4_fee_changed_related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
    INDEX dip4_fee_changed_related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, related_receipt_id)
ORDER BY (block_height, related_receipt_id)
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS silver_mv_dip4_fee_changed TO silver_dip4_fee_changed (
    block_height                UInt64,
    block_timestamp             DateTime64(9, 'UTC'),
    block_hash                  String,
    contract_id                 String,
    execution_status            String,
    version                     String,
    standard                    String,
    event                       String,
    related_receipt_id          String,
    related_receipt_predecessor_id String,
    related_receipt_receiver_id String,
    old_fee                     String,
    new_fee                     String
) AS
WITH decoded_events AS (
    SELECT *, data AS data_row
    FROM events
    WHERE (contract_id IN ('defuse-alpha.near', 'intents.near')) AND (standard = 'dip4') AND (event = 'fee_changed') AND (block_timestamp >= '2025-02-12 23:50:00')
)
SELECT block_height, block_timestamp, block_hash, contract_id, execution_status, version, standard, event, related_receipt_id, related_receipt_predecessor_id, related_receipt_receiver_id, coalesce(JSON_VALUE(data_row, '$.old_fee'), '') AS old_fee, coalesce(JSON_VALUE(data_row, '$.new_fee'), '') AS new_fee
FROM decoded_events
SETTINGS function_json_value_return_type_allow_nullable = true, function_json_value_return_type_allow_complex = true;
