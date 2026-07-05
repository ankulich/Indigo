-- Create database and tables for processed ticks

-- Connect to the database
\c ExchangeDb;

-- Create ticks table for storing processed tick data
CREATE TABLE IF NOT EXISTS ticks (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    ticker VARCHAR(50) NOT NULL,
    price DECIMAL(18,8) NOT NULL,
    volume DECIMAL(18,8) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_ticks_source ON ticks(source);
CREATE INDEX IF NOT EXISTS idx_ticks_ticker ON ticks(ticker);
CREATE INDEX IF NOT EXISTS idx_ticks_timestamp ON ticks(timestamp);

-- Create a materialized view for aggregated data (optional)
CREATE MATERIALIZED VIEW IF NOT EXISTS tick_aggregates AS
SELECT 
    source,
    ticker,
    COUNT(*) as tick_count,
    AVG(price) as avg_price,
    MIN(timestamp) as first_timestamp,
    MAX(timestamp) as last_timestamp
FROM ticks
GROUP BY source, ticker;

-- Refresh the materialized view periodically (this would be done by a scheduled job)
CREATE INDEX IF NOT EXISTS idx_tick_aggregates_source_ticker ON tick_aggregates(source, ticker);

COMMIT;