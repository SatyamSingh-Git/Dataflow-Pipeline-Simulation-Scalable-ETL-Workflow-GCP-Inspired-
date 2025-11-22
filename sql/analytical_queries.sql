-- Analytical Queries for Transaction Data

-- Query 1: Top spending users
SELECT 
    user_id,
    SUM(total_amount) as total_spent,
    SUM(transaction_count) as total_transactions,
    AVG(avg_amount) as average_transaction
FROM analytics.transaction_summary
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;

-- Query 2: Spending by category
SELECT 
    category,
    SUM(total_amount) as total_amount,
    SUM(transaction_count) as transaction_count,
    AVG(avg_amount) as avg_amount
FROM analytics.transaction_summary
GROUP BY category
ORDER BY total_amount DESC;

-- Query 3: High-value transactions
SELECT 
    user_id,
    merchant,
    amount,
    amount_usd,
    category,
    timestamp
FROM analytics.processed_transactions
WHERE is_high_value = TRUE
ORDER BY amount_usd DESC
LIMIT 20;

-- Query 4: Daily transaction trends
SELECT 
    DATE(timestamp) as transaction_date,
    COUNT(*) as transaction_count,
    SUM(amount_usd) as total_amount_usd,
    AVG(amount_usd) as avg_amount_usd
FROM analytics.processed_transactions
GROUP BY transaction_date
ORDER BY transaction_date DESC;

-- Query 5: User spending by category over time (example with user_0001, replace with actual user_id)
SELECT 
    user_id,
    category,
    summary_date,
    total_amount,
    transaction_count
FROM analytics.transaction_summary
WHERE user_id = 'user_0001'  -- Replace with desired user_id parameter
ORDER BY summary_date DESC, total_amount DESC;

-- Query 6: Merchant performance
SELECT 
    merchant,
    category,
    COUNT(*) as transaction_count,
    SUM(amount_usd) as total_revenue,
    AVG(amount_usd) as avg_transaction
FROM analytics.processed_transactions
WHERE status = 'completed'
GROUP BY merchant, category
ORDER BY total_revenue DESC
LIMIT 15;

-- Query 7: Transaction status distribution
SELECT 
    status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM analytics.raw_transactions
GROUP BY status
ORDER BY count DESC;
