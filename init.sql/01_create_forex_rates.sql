-- ============================================================================
-- Forex Rates Table Creation Script
-- ============================================================================
-- Purpose: Store foreign exchange (forex) daily, weekly, or monthly rates
-- Data Source: Alpha Vantage API
-- Updated: 2026-01-07
-- ============================================================================

-- Drop table if exists (comment out for production)
-- DROP TABLE IF EXISTS public.forex_rates CASCADE;

-- Create forex_rates table
CREATE TABLE IF NOT EXISTS public.forex_rates (
    -- Primary Key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Currency Pair Information
    from_currency VARCHAR(3) NOT NULL,
    to_currency VARCHAR(3) NOT NULL,
    
    -- Date Information
    date DATE NOT NULL,
    
    -- OHLC (Open, High, Low, Close) Exchange Rates
    open NUMERIC(15, 6) NOT NULL,
    high NUMERIC(15, 6) NOT NULL,
    low NUMERIC(15, 6) NOT NULL,
    close NUMERIC(15, 6) NOT NULL,
    
    -- Trading Volume
    volume BIGINT DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) DEFAULT 'alpha_vantage',
    
    -- Constraints
    CONSTRAINT unique_forex_pair_date UNIQUE (from_currency, to_currency, date),
    CONSTRAINT valid_high_low_close CHECK (high >= low),
    CONSTRAINT valid_high_open CHECK (high >= open),
    CONSTRAINT valid_low_open CHECK (low <= open),
    CONSTRAINT valid_close_high CHECK (close <= high),
    CONSTRAINT valid_close_low CHECK (close >= low),
    CONSTRAINT positive_rates CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0),
    CONSTRAINT volume_non_negative CHECK (volume >= 0),
    CONSTRAINT valid_currency_codes CHECK (
        from_currency ~ '^[A-Z]{3}$' AND 
        to_currency ~ '^[A-Z]{3}$' AND
        from_currency != to_currency
    )
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_forex_rates_pair ON public.forex_rates (from_currency, to_currency);
CREATE INDEX IF NOT EXISTS idx_forex_rates_date ON public.forex_rates (date DESC);
CREATE INDEX IF NOT EXISTS idx_forex_rates_pair_date ON public.forex_rates (from_currency, to_currency, date DESC);
CREATE INDEX IF NOT EXISTS idx_forex_rates_created_at ON public.forex_rates (created_at DESC);

-- Create index for upsert operations (for conflict resolution)
CREATE UNIQUE INDEX IF NOT EXISTS idx_forex_rates_upsert 
    ON public.forex_rates (from_currency, to_currency, date) 
    WHERE from_currency IS NOT NULL AND to_currency IS NOT NULL AND date IS NOT NULL;

-- Enable Row Level Security (optional, uncomment if needed)
-- ALTER TABLE public.forex_rates ENABLE ROW LEVEL SECURITY;

-- Add table comments
COMMENT ON TABLE public.forex_rates IS 'Foreign exchange daily/weekly/monthly rates from Alpha Vantage API';
COMMENT ON COLUMN public.forex_rates.id IS 'Unique identifier (UUID)';
COMMENT ON COLUMN public.forex_rates.from_currency IS 'Base currency code (ISO 4217, e.g., EUR)';
COMMENT ON COLUMN public.forex_rates.to_currency IS 'Quote currency code (ISO 4217, e.g., USD)';
COMMENT ON COLUMN public.forex_rates.date IS 'Trading date';
COMMENT ON COLUMN public.forex_rates.open IS 'Opening exchange rate (6 decimal places)';
COMMENT ON COLUMN public.forex_rates.high IS 'Highest exchange rate for the day';
COMMENT ON COLUMN public.forex_rates.low IS 'Lowest exchange rate for the day';
COMMENT ON COLUMN public.forex_rates.close IS 'Closing exchange rate';
COMMENT ON COLUMN public.forex_rates.volume IS 'Trading volume in millions of currency units';
COMMENT ON COLUMN public.forex_rates.created_at IS 'Record creation timestamp';
COMMENT ON COLUMN public.forex_rates.updated_at IS 'Record last update timestamp';
COMMENT ON COLUMN public.forex_rates.source IS 'Data source identifier';

-- Create trigger to auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_forex_rates_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_forex_rates_updated_at
    BEFORE UPDATE ON public.forex_rates
    FOR EACH ROW
    EXECUTE FUNCTION update_forex_rates_timestamp();

-- ============================================================================
-- Sample Queries
-- ============================================================================

-- Get latest EUR/USD rates (last 30 days)
-- SELECT * FROM public.forex_rates
-- WHERE from_currency = 'EUR' AND to_currency = 'USD'
-- ORDER BY date DESC
-- LIMIT 30;

-- Get all available currency pairs
-- SELECT DISTINCT from_currency, to_currency
-- FROM public.forex_rates
-- ORDER BY from_currency, to_currency;

-- Calculate 30-day moving average for EUR/USD
-- SELECT 
--     date,
--     close,
--     AVG(close) OVER (
--         ORDER BY date 
--         ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
--     ) AS moving_avg_30d
-- FROM public.forex_rates
-- WHERE from_currency = 'EUR' AND to_currency = 'USD'
-- ORDER BY date DESC;

-- Find high volatility days (high > 1.02 * low)
-- SELECT 
--     from_currency || '/' || to_currency AS pair,
--     date,
--     open,
--     high,
--     low,
--     close,
--     ROUND(((high - low) / low * 100)::NUMERIC, 2) AS volatility_pct
-- FROM public.forex_rates
-- WHERE (high - low) / low > 0.02
-- ORDER BY volatility_pct DESC;
