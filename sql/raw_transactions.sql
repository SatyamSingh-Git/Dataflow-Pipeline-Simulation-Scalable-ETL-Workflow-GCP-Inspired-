-- Raw Transactions Table Schema
-- Stores unprocessed transaction data as received from Pub/Sub

CREATE TABLE IF NOT EXISTS analytics.raw_transactions (
    transaction_id STRING NOT NULL,
    user_id STRING NOT NULL,
    amount FLOAT64 NOT NULL,
    currency STRING NOT NULL,
    merchant STRING,
    timestamp TIMESTAMP NOT NULL,
    location STRING,
    status STRING NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY user_id, merchant;

-- Indexes for efficient querying
CREATE INDEX idx_raw_transactions_user ON analytics.raw_transactions(user_id);
CREATE INDEX idx_raw_transactions_merchant ON analytics.raw_transactions(merchant);
CREATE INDEX idx_raw_transactions_timestamp ON analytics.raw_transactions(timestamp);
