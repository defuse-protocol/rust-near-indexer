# NEAR Defuse Custom Indexer

This project is a Rust-based indexer that processes blockchain events from NEAR Protocol and inserts them into a Clickhouse database for efficient querying. It uses [NEAR Lake Framework](https://github.com/near/near-lake-framework) for streaming blockchain data and stores structured events in a Clickhouse database.

## Features

- Stream NEAR blockchain events starting from a specified block height.
- Filter and structure events based on specific standards.
- Store events in a Clickhouse database.
- Persist transaction cache to Redis with automatic expiration after 50 blocks.

## Requirements

1. [Rust](https://www.rust-lang.org/tools/install) (1.76.0 version recommended)
2. [Clickhouse](https://clickhouse.com/docs/en/quick-start#self-managed-install) database server
3. Environment variables for configuration

### Build the project

```bash
cargo build --release
```

## Environment Configuration

Before running the indexer, ensure that all required environment variables are set:

| Variable                | Description                     | Default Value            |
| ----------------------- | ------------------------------- | ------------------------ |
| `CLICKHOUSE_URL`        | Clickhouse server URL           | `http://localhost:18123` |
| `CLICKHOUSE_USER`       | Username for Clickhouse         | -                        |
| `CLICKHOUSE_PASSWORD`   | Password for Clickhouse         | -                        |
| `CLICKHOUSE_DB`         | Clickhouse database name        | `mainnet`                |
| `BLOCK_HEIGHT`          | Start block height for indexing | -                        |
| `AWS_ACCESS_KEY_ID`     | AWS access key ID               | -                        |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key           | -                        |
| `REDIS_URL`             | Redis connection URL (optional) | -                        |

Example of setting up environment variables (replace with your actual values):

```bash
export CLICKHOUSE_URL="http://localhost:18123"
export CLICKHOUSE_USER="<your-clickhouse-username>"
export CLICKHOUSE_PASSWORD="<your-clickhouse-password>"
export CLICKHOUSE_DB="mainnet"
export BLOCK_HEIGHT="130636886"
export AWS_ACCESS_KEY_ID="<your-aws-access-key-id>"
export AWS_SECRET_ACCESS_KEY="<your-aws-secret-access-key>"
export REDIS_URL="redis://127.0.0.1:6379"
```

## Usage

1. Start the Clickhouse server.
2. Run the application:

   ```bash
   cargo run --release
   ```

The application will:

- Connect to the Clickhouse server.
- Start reading blockchain events from the specified `BLOCK_HEIGHT` (or the last processed height).
- Filter and insert selected events into the Clickhouse database.

### Optional Command-line Execution

You can pass `BLOCK_HEIGHT` when running the program to override the default or previously set block height.

```bash
BLOCK_HEIGHT=130636886 cargo run --release
```

## Project Structure

The project is structured as follows:

- `main.rs`: Initializes the application, connects to Clickhouse, and starts the indexer.
- `database.rs`: Contains functions for connecting to Clickhouse and managing data.
- `event_handler.rs`: Processes streamed events and filters specific event standards.

## Clickhouse schema

Here is the Clickhouse schema to run the indexer:

```sql
CREATE TABLE defuse_assets (
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

CREATE TABLE events (
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
CREATE TABLE silver_nep_245_events (
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
CREATE MATERIALIZED VIEW mv_silver_nep_245_events TO silver_nep_245_events (
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

CREATE TABLE silver_dip4_token_diff (
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
CREATE MATERIALIZED VIEW mv_silver_dip4_token_diff TO silver_dip4_token_diff (
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


CREATE TABLE silver_dip4_public_keys (
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
CREATE MATERIALIZED VIEW mv_silver_dip4_public_keys TO silver_dip4_public_keys (
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
CREATE TABLE silver_dip4_intents_executed (
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
CREATE MATERIALIZED VIEW mv_silver_dip4_intents_executed TO silver_dip4_intents_executed (
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


CREATE TABLE silver_dip4_fee_changed (
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
CREATE MATERIALIZED VIEW silver_mv_dip4_fee_changed TO silver_dip4_fee_changed (
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

CREATE VIEW gold_view_intents_metrics (
    day Date,
    symbol String,
    referral String,
    blockchain String,
    transfer_volume Nullable(Float64),
    deposits Nullable(Float64),
    withdraws Nullable(Float64),
    netflow Nullable(Float64)
) AS
WITH decoded AS (
    SELECT DISTINCT e.block_timestamp, e.block_hash, e.event, e.memo, e.old_owner_id, e.new_owner_id, e.token_id,
           (e.amount / pow(10, a.decimals)) * a.price AS usd_value,
           a.symbol, a.blockchain, d.referral
    FROM silver_nep_245_events AS e
    LEFT JOIN silver_dip4_token_diff AS d ON d.related_receipt_id = e.related_receipt_id
    LEFT JOIN defuse_assets AS a ON (CAST(e.block_timestamp, 'date') = CAST(a.price_updated_at, 'date')) AND (e.token_id = a.defuse_asset_id)
    WHERE NOT ((length(referral) = 0) AND (length(memo) = 0))
)
SELECT CAST(e.block_timestamp, 'date') AS day, symbol, coalesce(referral, 'Others') AS referral, blockchain,
       sum(multiIf(e.event = 'mt_transfer', usd_value, NULL)) AS transfer_volume,
       sum(multiIf(e.event = 'mt_mint', usd_value, NULL)) AS deposits,
       sum(multiIf(e.event = 'mt_burn', usd_value, NULL)) * -1 AS withdraws,
       sum(multiIf(e.event = 'mt_mint', usd_value, e.event = 'mt_burn', usd_value * -1, NULL)) AS netflow
FROM decoded
WHERE (symbol != '') AND (blockchain != '')
GROUP BY ALL
ORDER BY 1 ASC;

CREATE TABLE transactions (
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

CREATE TABLE receipts (
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

CREATE TABLE execution_outcomes (
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
PRIMARY KEY (block_height, parent_transaction_hash)
ORDER BY (block_height, parent_transaction_hash)
SETTINGS index_granularity = 8192;

```
