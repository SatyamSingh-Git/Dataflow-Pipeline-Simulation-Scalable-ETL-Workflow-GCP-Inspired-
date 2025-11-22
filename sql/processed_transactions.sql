-- Processed Transactions Table Schema
-- Stores enriched and transformed transaction data

CREATE TABLE IF NOT EXISTS analytics.processed_transactions (
    transaction_id STRING NOT NULL,
    user_id STRING NOT NULL,
    amount FLOAT64 NOT NULL,
    currency STRING NOT NULL,
    merchant STRING,
    timestamp TIMESTAMP NOT NULL,
    location STRING,
    status STRING NOT NULL,
    category STRING,
    is_high_value BOOLEAN NOT NULL,
    processing_timestamp TIMESTAMP NOT NULL,
    amount_usd FLOAT64
)
PARTITION BY DATE(timestamp)
CLUSTER BY user_id, category, is_high_value;

-- Indexes for efficient querying
CREATE INDEX idx_processed_transactions_user ON analytics.processed_transactions(user_id);
CREATE INDEX idx_processed_transactions_category ON analytics.processed_transactions(category);
CREATE INDEX idx_processed_transactions_high_value ON analytics.processed_transactions(is_high_value);
