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
) PARTITION BY RANGE (block_timestamp);

-- Primary uniqueness constraint (must include partition key)
CREATE UNIQUE INDEX IF NOT EXISTS silver_dip4_transfers_pk
    ON silver_dip4_transfers (
        block_timestamp, block_height, related_receipt_id, event,
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

-- Pre-create monthly partitions: Dec 2024 through Dec 2026
CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2024_12
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_01
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_02
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_03
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_04
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_05
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_06
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_07
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_08
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_09
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_10
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_11
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2025_12
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_01
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_02
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_03
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_04
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_05
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_06
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_07
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_08
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_09
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_10
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_11
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

CREATE TABLE IF NOT EXISTS silver_dip4_transfers_2026_12
    PARTITION OF silver_dip4_transfers
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');
