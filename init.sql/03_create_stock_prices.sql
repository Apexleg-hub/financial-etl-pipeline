-- ============================================================================
-- Financial ETL Pipeline - Stock Prices Table
-- ============================================================================
-- Purpose: Store historical stock price data from Alpha Vantage and other sources
-- Table Name: public.stock_prices
-- Created: 2026-01-07
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.stock_prices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(12, 4) NOT NULL,
    high NUMERIC(12, 4) NOT NULL,
    low NUMERIC(12, 4) NOT NULL,
    close NUMERIC(12, 4) NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,
    adjusted_close NUMERIC(12, 4),
    source VARCHAR(50) NOT NULL DEFAULT 'alpha_vantage',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Constraints: Data Validity
    CONSTRAINT chk_high_gte_low CHECK (high >= low),
    CONSTRAINT chk_high_gte_open CHECK (high >= open),
    CONSTRAINT chk_high_gte_close CHECK (high >= close),
    CONSTRAINT chk_low_lte_open CHECK (low <= open),
    CONSTRAINT chk_low_lte_close CHECK (low <= close),
    CONSTRAINT chk_positive_prices CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0),
    CONSTRAINT chk_volume_non_negative CHECK (volume >= 0),
    CONSTRAINT chk_adjusted_close_positive CHECK (adjusted_close IS NULL OR adjusted_close > 0),
    UNIQUE(symbol, date),
    CONSTRAINT chk_symbol_format CHECK (symbol ~ '^[A-Z]{1,10}$')
);

-- ============================================================================
-- INDEXES: Query Optimization
-- ============================================================================

-- Index 1: Symbol lookup (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol 
    ON public.stock_prices(symbol);

-- Index 2: Date range queries
CREATE INDEX IF NOT EXISTS idx_stock_prices_date 
    ON public.stock_prices(date DESC);

-- Index 3: Composite index for symbol + date range queries
-- Useful for: "Get all AAPL prices between Jan 1 and Dec 31"
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date 
    ON public.stock_prices(symbol, date DESC);

-- Index 4: Creation timestamp (for tracking recent inserts)
CREATE INDEX IF NOT EXISTS idx_stock_prices_created_at 
    ON public.stock_prices(created_at DESC);

-- Index 5: Upsert optimization (most efficient conflict detection)
-- Used when: ON CONFLICT (symbol, date) DO UPDATE
CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_prices_upsert 
    ON public.stock_prices(symbol, date) 
    WHERE date >= CURRENT_DATE - INTERVAL '2 years';

-- Index 6: Source tracking (for data lineage queries)
CREATE INDEX IF NOT EXISTS idx_stock_prices_source 
    ON public.stock_prices(source);

-- ============================================================================
-- TRIGGER: Automatic Timestamp Management
-- ============================================================================

-- Create trigger function if it doesn't exist
CREATE OR REPLACE FUNCTION update_stock_prices_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop trigger if exists (to avoid conflicts)
DROP TRIGGER IF EXISTS trg_stock_prices_update_timestamp ON public.stock_prices;

-- Create trigger
CREATE TRIGGER trg_stock_prices_update_timestamp
BEFORE UPDATE ON public.stock_prices
FOR EACH ROW
EXECUTE FUNCTION update_stock_prices_timestamp();

-- ============================================================================
-- COMMENTS: Self-documenting Schema
-- ============================================================================

-- ============================================================================
-- EXAMPLE DATA
-- ============================================================================

-- Example 1: Insert Apple stock data
-- INSERT INTO public.stock_prices (symbol, date, open, high, low, close, volume, source)
-- VALUES ('AAPL', '2026-01-07', 246.50, 247.25, 245.80, 246.90, 52350000, 'alpha_vantage');

-- Example 2: Bulk insert multiple stocks
-- INSERT INTO public.stock_prices (symbol, date, open, high, low, close, volume)
-- VALUES
--     ('AAPL', '2026-01-07', 246.50, 247.25, 245.80, 246.90, 52350000),
--     ('MSFT', '2026-01-07', 428.15, 430.50, 427.80, 429.35, 18740000),
--     ('GOOGL', '2026-01-07', 175.40, 176.85, 174.95, 175.80, 12560000);

-- Example 3: Upsert pattern (used by Python ETL)
-- INSERT INTO public.stock_prices (symbol, date, open, high, low, close, volume, source)
-- VALUES ('AAPL', '2026-01-07', 246.50, 247.25, 245.80, 246.90, 52350000, 'alpha_vantage')
-- ON CONFLICT (symbol, date)
-- DO UPDATE SET
--     open = EXCLUDED.open,
--     high = EXCLUDED.high,
--     low = EXCLUDED.low,
--     close = EXCLUDED.close,
--     volume = EXCLUDED.volume,
--     source = EXCLUDED.source,
--     updated_at = CURRENT_TIMESTAMP;

-- ============================================================================
-- COMMON QUERIES
-- ============================================================================

-- Query 1: Get latest close price for a symbol
-- SELECT close FROM public.stock_prices 
-- WHERE symbol = 'AAPL' 
-- ORDER BY date DESC LIMIT 1;

-- Query 2: Get 30-day moving average
-- SELECT date, AVG(close) OVER (
--     ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
-- ) as ma_30
-- FROM public.stock_prices 
-- WHERE symbol = 'AAPL' AND date >= CURRENT_DATE - INTERVAL '90 days'
-- ORDER BY date DESC;

-- Query 3: Calculate daily returns
-- SELECT date, close,
--   LAG(close) OVER (ORDER BY date) as prev_close,
--   ((close - LAG(close) OVER (ORDER BY date)) / LAG(close) OVER (ORDER BY date)) * 100 as daily_return_pct
-- FROM public.stock_prices 
-- WHERE symbol = 'AAPL' AND date >= CURRENT_DATE - INTERVAL '60 days'
-- ORDER BY date DESC;

-- Query 4: Identify volatility (average true range)
-- SELECT symbol, date, high, low, close,
--   AVG(high - low) OVER (
--     PARTITION BY symbol 
--     ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
--   ) as atr_20
-- FROM public.stock_prices 
-- WHERE date >= CURRENT_DATE - INTERVAL '60 days'
-- ORDER BY symbol, date DESC;
