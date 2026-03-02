-- Gold layer views.
-- Executed on first ClickHouse container start via /docker-entrypoint-initdb.d/.

CREATE VIEW IF NOT EXISTS gold_view_intents_metrics (
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
SELECT CAST(block_timestamp, 'date') AS day, symbol, coalesce(referral, 'Others') AS referral, blockchain,
       sum(multiIf(event = 'mt_transfer', usd_value, NULL)) AS transfer_volume,
       sum(multiIf(event = 'mt_mint', usd_value, NULL)) AS deposits,
       sum(multiIf(event = 'mt_burn', usd_value, NULL)) * -1 AS withdraws,
       sum(multiIf(event = 'mt_mint', usd_value, event = 'mt_burn', usd_value * -1, NULL)) AS netflow
FROM decoded
WHERE (symbol != '') AND (blockchain != '')
GROUP BY ALL
ORDER BY 1 ASC;
