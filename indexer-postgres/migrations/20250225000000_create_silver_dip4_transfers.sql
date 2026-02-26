CREATE TABLE IF NOT EXISTS silver_dip4_transfers (
    block_height                   BIGINT NOT NULL,
    block_timestamp                TIMESTAMPTZ NOT NULL,
    block_hash                     TEXT NOT NULL,
    tx_hash                        TEXT NOT NULL,
    contract_id                    TEXT NOT NULL,
    execution_status               TEXT NOT NULL,
    version                        TEXT NOT NULL,
    standard                       TEXT NOT NULL,
    event                          TEXT NOT NULL,
    related_receipt_id             TEXT NOT NULL,
    related_receipt_receiver_id    TEXT NOT NULL,
    related_receipt_predecessor_id TEXT NOT NULL,
    memo                           TEXT,
    old_owner_id                   TEXT,
    new_owner_id                   TEXT,
    token_id                       TEXT,
    amount                         NUMERIC,
    intent_hash                    TEXT NOT NULL DEFAULT '',
    referral                       TEXT
);

-- Primary uniqueness constraint
CREATE UNIQUE INDEX IF NOT EXISTS silver_dip4_transfers_pk
    ON silver_dip4_transfers (
        block_height, related_receipt_id, event,
        COALESCE(old_owner_id, ''), COALESCE(new_owner_id, ''),
        COALESCE(token_id, '')
    );

-- Indexes for query patterns (cursor-pagination by timestamp + receipt, lookups by owner/receipt)
CREATE INDEX IF NOT EXISTS idx_silver_dip4_transfers_old_owner
    ON silver_dip4_transfers (old_owner_id, execution_status, block_timestamp DESC, related_receipt_id DESC);

CREATE INDEX IF NOT EXISTS idx_silver_dip4_transfers_new_owner
    ON silver_dip4_transfers (new_owner_id, execution_status, block_timestamp DESC, related_receipt_id DESC)
    WHERE memo IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_silver_dip4_transfers_receipt
    ON silver_dip4_transfers (related_receipt_id);
