-- Transaction Summary Table Schema
-- Stores aggregated transaction metrics by user and category

CREATE TABLE IF NOT EXISTS analytics.transaction_summary (
    user_id STRING NOT NULL,
    category STRING NOT NULL,
    total_amount FLOAT64 NOT NULL,
    transaction_count INT64 NOT NULL,
    avg_amount FLOAT64 NOT NULL,
    max_amount FLOAT64 NOT NULL,
    min_amount FLOAT64 NOT NULL,
    summary_date DATE NOT NULL
)
PARTITION BY summary_date
CLUSTER BY user_id, category;

-- Indexes for efficient querying
CREATE INDEX idx_summary_user ON analytics.transaction_summary(user_id);
CREATE INDEX idx_summary_category ON analytics.transaction_summary(category);
CREATE INDEX idx_summary_date ON analytics.transaction_summary(summary_date);
