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
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_ticks_source ON ticks(source);
CREATE INDEX IF NOT EXISTS idx_ticks_ticker ON ticks(ticker);
CREATE INDEX IF NOT EXISTS idx_ticks_timestamp ON ticks(timestamp);
CREATE INDEX IF NOT EXISTS idx_ticks_created_at ON ticks(created_at);

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

-- Add a trigger to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE OR REPLACE TRIGGER update_ticks_updated_at
    BEFORE UPDATE ON ticks
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

COMMIT;