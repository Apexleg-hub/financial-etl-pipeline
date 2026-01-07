-- ============================================================================
-- Financial ETL Pipeline - MT5 Forex Data Table
-- ============================================================================
-- Purpose: Store forex data from MetaTrader 5 with precise volume information
-- MT5 provides tick-level volume and granular timeframes (M1, M5, M15, H1, D1, etc)
-- Table Name: public.forex_mt5
-- Created: 2026-01-07
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.forex_mt5 (
    -- ========================================================================
    -- Primary Key & Identifiers
    -- ========================================================================
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_currency VARCHAR(3) NOT NULL COMMENT 'Base currency code (ISO 4217, e.g., EUR)',
    to_currency VARCHAR(3) NOT NULL COMMENT 'Quote currency code (ISO 4217, e.g., USD)',
    
    -- ========================================================================
    -- Time Series Data (More granular than daily Alpha Vantage)
    -- ========================================================================
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL COMMENT 'Precise timestamp of candle (UTC)',
    open NUMERIC(15, 6) NOT NULL COMMENT 'Opening rate',
    high NUMERIC(15, 6) NOT NULL COMMENT 'Highest rate during the period',
    low NUMERIC(15, 6) NOT NULL COMMENT 'Lowest rate during the period',
    close NUMERIC(15, 6) NOT NULL COMMENT 'Closing rate',
    
    -- ========================================================================
    -- Volume Data (MT5 specific - Alpha Vantage doesn't provide this)
    -- ========================================================================
    volume BIGINT NOT NULL DEFAULT 0 COMMENT 'Tick volume (number of ticks during period)',
    real_volume BIGINT COMMENT 'Real volume (if broker provides it)',
    
    -- ========================================================================
    -- MT5 Specific Metadata
    -- ========================================================================
    timeframe VARCHAR(10) NOT NULL DEFAULT 'D1' COMMENT 'Candle timeframe: M1, M5, M15, M30, H1, H4, D1, W1, MN1',
    broker VARCHAR(100) COMMENT 'MT5 broker name (e.g., "IC Markets", "Pepperstone")',
    account_type VARCHAR(50) COMMENT 'Account type (Real, Demo, Backtest)',
    
    -- ========================================================================
    -- Data Quality Flags
    -- ========================================================================
    is_bid BOOLEAN DEFAULT FALSE COMMENT 'TRUE if data is bid prices, FALSE if ask prices',
    tick_count INTEGER COMMENT 'Number of ticks during period (for volume validation)',
    
    -- ========================================================================
    -- Metadata
    -- ========================================================================
    source VARCHAR(50) NOT NULL DEFAULT 'mt5' COMMENT 'Data source (always mt5 for this table)',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- ========================================================================
    -- Constraints: Data Validity
    -- ========================================================================
    CONSTRAINT chk_high_gte_low CHECK (high >= low),
    CONSTRAINT chk_high_gte_open CHECK (high >= open),
    CONSTRAINT chk_high_gte_close CHECK (high >= close),
    CONSTRAINT chk_low_lte_open CHECK (low <= open),
    CONSTRAINT chk_low_lte_close CHECK (low <= close),
    CONSTRAINT chk_positive_rates CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0),
    CONSTRAINT chk_volume_non_negative CHECK (volume >= 0),
    CONSTRAINT chk_real_volume_non_negative CHECK (real_volume IS NULL OR real_volume >= 0),
    CONSTRAINT chk_tick_count_non_negative CHECK (tick_count IS NULL OR tick_count >= 0),
    CONSTRAINT chk_valid_timeframe CHECK (
        timeframe IN ('M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1', 'W1', 'MN1')
    ),
    CONSTRAINT chk_currency_codes CHECK (
        from_currency ~ '^[A-Z]{3}$' AND 
        to_currency ~ '^[A-Z]{3}$' AND
        from_currency != to_currency
    ),
    
    -- ========================================================================
    -- Constraint: Ensure unique currency-timestamp-timeframe-broker combination
    -- Critical for upsert operations and preventing duplicates
    -- Different brokers may have slightly different prices/volumes
    -- ========================================================================
    UNIQUE(from_currency, to_currency, timestamp, timeframe, COALESCE(broker, 'default'))
);

-- ============================================================================
-- INDEXES: Query Optimization
-- ============================================================================

-- Index 1: Currency pair lookup (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_forex_mt5_pair 
    ON public.forex_mt5(from_currency, to_currency);

-- Index 2: Timestamp range queries (recent data first)
CREATE INDEX IF NOT EXISTS idx_forex_mt5_timestamp 
    ON public.forex_mt5(timestamp DESC);

-- Index 3: Composite index for pair + timeframe + timestamp
-- Useful for: "Get all 1-hour EUR/USD candles for the last 30 days"
CREATE INDEX IF NOT EXISTS idx_forex_mt5_pair_timeframe_timestamp 
    ON public.forex_mt5(from_currency, to_currency, timeframe, timestamp DESC);

-- Index 4: Timeframe-based queries (common for technical analysis)
CREATE INDEX IF NOT EXISTS idx_forex_mt5_timeframe 
    ON public.forex_mt5(timeframe);

-- Index 5: Broker tracking (for broker-specific analysis)
CREATE INDEX IF NOT EXISTS idx_forex_mt5_broker 
    ON public.forex_mt5(broker) 
    WHERE broker IS NOT NULL;

-- Index 6: Creation timestamp (for tracking recent inserts)
CREATE INDEX IF NOT EXISTS idx_forex_mt5_created_at 
    ON public.forex_mt5(created_at DESC);

-- Index 7: Upsert optimization (composite key for conflict detection)
-- Optimized for the unique constraint
CREATE UNIQUE INDEX IF NOT EXISTS idx_forex_mt5_upsert 
    ON public.forex_mt5(from_currency, to_currency, timestamp, timeframe, COALESCE(broker, 'default'));

-- Index 8: Volume queries (for volume analysis)
CREATE INDEX IF NOT EXISTS idx_forex_mt5_volume 
    ON public.forex_mt5(volume DESC) 
    WHERE volume > 0;

-- ============================================================================
-- TRIGGER: Automatic Timestamp Management
-- ============================================================================

-- Create trigger function if it doesn't exist
CREATE OR REPLACE FUNCTION update_forex_mt5_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop trigger if exists (to avoid conflicts)
DROP TRIGGER IF EXISTS trg_forex_mt5_update_timestamp ON public.forex_mt5;

-- Create trigger
CREATE TRIGGER trg_forex_mt5_update_timestamp
BEFORE UPDATE ON public.forex_mt5
FOR EACH ROW
EXECUTE FUNCTION update_forex_mt5_timestamp();

-- ============================================================================
-- COMMENTS: Self-documenting Schema
-- ============================================================================

COMMENT ON TABLE public.forex_mt5 IS 
'MetaTrader 5 forex data with precise timestamp and volume information.
Unlike Alpha Vantage (daily data only), MT5 provides tick volume and multiple timeframes.
Supports M1 (1-minute) through MN1 (monthly) candles.
Essential for technical analysis, high-frequency monitoring, and broker-specific analysis.
Allows backtesting against real volume data from active brokers.
Different from forex_rates table due to granularity and availability of volume data.
Indexes optimized for timeframe-based queries and technical analysis patterns.';

COMMENT ON COLUMN public.forex_mt5.id IS
'Unique identifier (UUID). Primary key for record-level operations.';

COMMENT ON COLUMN public.forex_mt5.from_currency IS
'Base currency code (3 uppercase letters). Examples: EUR, GBP, USD, JPY, CHF.
Combined with to_currency, timestamp, and timeframe for uniqueness.';

COMMENT ON COLUMN public.forex_mt5.to_currency IS
'Quote currency code (3 uppercase letters). Examples: EUR, GBP, USD, JPY, CHF.
Combined with from_currency, timestamp, and timeframe for uniqueness.';

COMMENT ON COLUMN public.forex_mt5.timestamp IS
'Precise timestamp of candle close in UTC. Format: YYYY-MM-DD HH:MM:SS±TZ.
More granular than Alpha Vantage''s daily dates.
Examples: 2026-01-07 12:34:00+00:00 (for M1), 2026-01-07 16:00:00+00:00 (for H1).';

COMMENT ON COLUMN public.forex_mt5.open IS
'Opening rate for the candle period. Usually equals previous candle close.
Precision: 6 decimal places (accommodates most forex pairs).';

COMMENT ON COLUMN public.forex_mt5.high IS
'Highest rate reached during the candle period.
Must be >= open, close, and low (enforced by constraints).';

COMMENT ON COLUMN public.forex_mt5.low IS
'Lowest rate reached during the candle period.
Must be <= open, close, and high (enforced by constraints).';

COMMENT ON COLUMN public.forex_mt5.close IS
'Closing rate at end of candle period.
Used for most technical analysis calculations and price-based signals.';

COMMENT ON COLUMN public.forex_mt5.volume IS
'Tick volume: Number of ticks (price changes) during the period.
All MT5 brokers provide this. Used for volume analysis and bar strength.
Zero indicates no trading (unusual for liquid pairs during market hours).';

COMMENT ON COLUMN public.forex_mt5.real_volume IS
'Real volume in base currency units (if broker provides it).
Not all brokers provide real volume; typically institutional brokers do.
NULL indicates broker doesn''t provide, only tick volume available.';

COMMENT ON COLUMN public.forex_mt5.timeframe IS
'Candle timeframe: M1 (1-min), M5 (5-min), M15 (15-min), M30 (30-min),
H1 (hourly), H4 (4-hour), D1 (daily), W1 (weekly), MN1 (monthly).
Critical for technical analysis strategies at different time horizons.';

COMMENT ON COLUMN public.forex_mt5.broker IS
'MT5 broker identifier (e.g., "IC Markets", "Pepperstone", "XM", "Forex.com").
Multiple brokers may have slightly different prices due to liquidity/spread differences.
Used to analyze broker-specific patterns or compare data sources.
NULL indicates data aggregated from multiple brokers or broker unknown.';

COMMENT ON COLUMN public.forex_mt5.account_type IS
'Account classification: "Real" (live trading), "Demo" (simulated), "Backtest" (historical).
Important to distinguish live vs. backtesting data for strategy validation.
Real accounts are most valuable for realistic price/volume data.';

COMMENT ON COLUMN public.forex_mt5.is_bid IS
'TRUE = bid prices (lower, ask side), FALSE = ask prices (higher, bid side) or mid-prices.
Forex typically tracks bid prices. Important for accurate spread and slippage calculations.';

COMMENT ON COLUMN public.forex_mt5.tick_count IS
'Number of individual ticks aggregated into this candle.
Tick volume / tick_count ≈ average price movement per tick (volatility metric).
Useful for detecting low-liquidity periods or data quality issues.';

COMMENT ON COLUMN public.forex_mt5.source IS
'Data source identifier. Always "mt5" for this table (value for compatibility).
Allows unified queries across forex_rates and forex_mt5 tables using UNION.';

COMMENT ON COLUMN public.forex_mt5.created_at IS
'Timestamp when record was first inserted into our database.
Set once at creation, never modified. Used for audit trail.
May differ significantly from candle timestamp (e.g., backtest data inserted later).';

COMMENT ON COLUMN public.forex_mt5.updated_at IS
'Timestamp of last modification. Automatically updated by trigger on any changes.
Used to identify stale records and track when data was refreshed.';

-- ============================================================================
-- EXAMPLE DATA
-- ============================================================================

-- Example 1: Insert a single 1-minute candle
-- INSERT INTO public.forex_mt5 (
--     from_currency, to_currency, timestamp, open, high, low, close,
--     volume, timeframe, broker, source
-- ) VALUES (
--     'EUR', 'USD',
--     '2026-01-07 12:34:00+00:00',
--     1.08520, 1.08540, 1.08500, 1.08535,
--     1250, 'M1', 'IC Markets', 'mt5'
-- );

-- Example 2: Bulk insert hourly candles for multiple pairs
-- INSERT INTO public.forex_mt5 (
--     from_currency, to_currency, timestamp, open, high, low, close,
--     volume, timeframe, broker
-- ) VALUES
--     ('EUR', 'USD', '2026-01-07 12:00:00+00:00', 1.08500, 1.08620, 1.08480, 1.08590, 45320, 'H1', 'IC Markets'),
--     ('EUR', 'USD', '2026-01-07 13:00:00+00:00', 1.08590, 1.08700, 1.08560, 1.08650, 38950, 'H1', 'IC Markets'),
--     ('GBP', 'USD', '2026-01-07 12:00:00+00:00', 1.26480, 1.26650, 1.26450, 1.26620, 52100, 'H1', 'IC Markets'),
--     ('GBP', 'USD', '2026-01-07 13:00:00+00:00', 1.26620, 1.26750, 1.26580, 1.26700, 41200, 'H1', 'IC Markets');

-- Example 3: Upsert pattern (used by Python ETL for handling real-time data)
-- INSERT INTO public.forex_mt5 (
--     from_currency, to_currency, timestamp, open, high, low, close,
--     volume, timeframe, broker, source
-- ) VALUES (
--     'EUR', 'USD',
--     '2026-01-07 12:34:00+00:00',
--     1.08520, 1.08540, 1.08500, 1.08535,
--     1250, 'M1', 'IC Markets', 'mt5'
-- ) ON CONFLICT (from_currency, to_currency, timestamp, timeframe, COALESCE(broker, 'default'))
-- DO UPDATE SET
--     open = EXCLUDED.open,
--     high = EXCLUDED.high,
--     low = EXCLUDED.low,
--     close = EXCLUDED.close,
--     volume = EXCLUDED.volume,
--     updated_at = CURRENT_TIMESTAMP;

-- ============================================================================
-- COMMON QUERIES FOR TECHNICAL ANALYSIS
-- ============================================================================

-- Query 1: Get latest hourly candle for EUR/USD
-- SELECT timestamp, open, high, low, close, volume
-- FROM public.forex_mt5
-- WHERE from_currency = 'EUR' AND to_currency = 'USD' AND timeframe = 'H1'
-- ORDER BY timestamp DESC LIMIT 1;

-- Query 2: 20-period moving average (MA20) for M15 timeframe
-- SELECT timestamp, close,
--   AVG(close) OVER (
--     ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
--   ) as ma_20
-- FROM public.forex_mt5
-- WHERE from_currency = 'EUR' AND to_currency = 'USD' AND timeframe = 'M15'
--   AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '3 days'
-- ORDER BY timestamp DESC;

-- Query 3: Volume analysis (average volume per timeframe)
-- SELECT timeframe,
--   AVG(volume) as avg_volume,
--   MIN(volume) as min_volume,
--   MAX(volume) as max_volume,
--   STDDEV(volume) as volume_stddev
-- FROM public.forex_mt5
-- WHERE from_currency = 'EUR' AND to_currency = 'USD'
--   AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
-- GROUP BY timeframe
-- ORDER BY CASE 
--   WHEN timeframe = 'M1' THEN 1
--   WHEN timeframe = 'M5' THEN 2
--   WHEN timeframe = 'M15' THEN 3
--   WHEN timeframe = 'H1' THEN 4
--   WHEN timeframe = 'D1' THEN 5
--   END;

-- Query 4: High-volume bars (potential support/resistance)
-- SELECT timestamp, open, high, low, close, volume,
--   AVG(volume) OVER (
--     ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
--   ) as ma_20_volume
-- FROM public.forex_mt5
-- WHERE from_currency = 'EUR' AND to_currency = 'USD' AND timeframe = 'D1'
--   AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '90 days'
--   AND volume > 1.5 * AVG(volume) OVER (
--     ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
--   )
-- ORDER BY timestamp DESC;

-- Query 5: Daily high/low range
-- SELECT DATE(timestamp) as trading_date,
--   MAX(high) as daily_high,
--   MIN(low) as daily_low,
--   MAX(high) - MIN(low) as daily_range,
--   SUM(volume) as total_volume
-- FROM public.forex_mt5
-- WHERE from_currency = 'EUR' AND to_currency = 'USD' AND timeframe = 'H1'
--   AND timestamp >= CURRENT_DATE - INTERVAL '30 days'
-- GROUP BY DATE(timestamp)
-- ORDER BY trading_date DESC;

-- Query 6: Broker comparison (same pair, different brokers)
-- SELECT broker, timestamp, open, high, low, close, volume
-- FROM public.forex_mt5
-- WHERE from_currency = 'EUR' AND to_currency = 'USD' AND timeframe = 'H1'
--   AND timestamp = (SELECT MAX(timestamp) FROM public.forex_mt5 
--                    WHERE from_currency = 'EUR' AND to_currency = 'USD' AND timeframe = 'H1')
-- ORDER BY broker;

-- Query 7: Volatility analysis (ATR - Average True Range)
-- SELECT timestamp, close,
--   AVG(GREATEST(high - low, ABS(high - LAG(close) OVER (ORDER BY timestamp)), ABS(low - LAG(close) OVER (ORDER BY timestamp))))
--     OVER (ORDER BY timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as atr_14
-- FROM public.forex_mt5
-- WHERE from_currency = 'EUR' AND to_currency = 'USD' AND timeframe = 'H1'
--   AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
-- ORDER BY timestamp DESC;
