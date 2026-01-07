-- ============================================================================
-- FINANCIAL ETL PIPELINE - MASTER DATABASE INITIALIZATION
-- ============================================================================
-- Complete table setup for Supabase PostgreSQL
-- Created: 2026-01-07
-- Status: Production Ready
-- ============================================================================

-- ============================================================================
-- 1. STOCK PRICES TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS stock_prices (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(12,4) NOT NULL,
    high NUMERIC(12,4) NOT NULL,
    low NUMERIC(12,4) NOT NULL,
    close NUMERIC(12,4) NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,
    adjusted_close NUMERIC(12,4),
    source VARCHAR(50) DEFAULT 'alpha_vantage',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, date, source)
);

-- ============================================================================
-- 2. FOREX RATES TABLE (Alpha Vantage - Daily, No Volume)
-- ============================================================================

CREATE TABLE IF NOT EXISTS forex_rates (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    from_currency VARCHAR(3) NOT NULL,
    to_currency VARCHAR(3) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(15,6) NOT NULL,
    high NUMERIC(15,6) NOT NULL,
    low NUMERIC(15,6) NOT NULL,
    close NUMERIC(15,6) NOT NULL,
    volume BIGINT DEFAULT 0,
    source VARCHAR(50) DEFAULT 'alpha_vantage',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(from_currency, to_currency, date, source)
);

-- ============================================================================
-- 3. FOREX MT5 TABLE (MetaTrader 5 - With Volume, Multiple Timeframes)
-- ============================================================================

CREATE TABLE IF NOT EXISTS forex_mt5 (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    from_currency VARCHAR(3) NOT NULL,
    to_currency VARCHAR(3) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC(15,6) NOT NULL,
    high NUMERIC(15,6) NOT NULL,
    low NUMERIC(15,6) NOT NULL,
    close NUMERIC(15,6) NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,
    real_volume BIGINT,
    timeframe VARCHAR(10) NOT NULL DEFAULT 'D1',
    broker VARCHAR(100),
    source VARCHAR(50) DEFAULT 'mt5',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(from_currency, to_currency, timestamp, timeframe, COALESCE(broker, 'default'))
);

-- ============================================================================
-- 4. CRYPTOCURRENCY PRICES TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS crypto_prices (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC(20,8),
    high NUMERIC(20,8),
    low NUMERIC(20,8),
    close NUMERIC(20,8) NOT NULL,
    volume NUMERIC(30,8) NOT NULL,
    source VARCHAR(50) DEFAULT 'binance',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, exchange, timestamp, source)
);

-- ============================================================================
-- 5. ECONOMIC INDICATORS TABLE (FRED)
-- ============================================================================

CREATE TABLE IF NOT EXISTS economic_indicators (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    indicator_code VARCHAR(50) NOT NULL,
    indicator_name VARCHAR(255),
    date DATE NOT NULL,
    value NUMERIC(20,6) NOT NULL,
    unit VARCHAR(100),
    frequency VARCHAR(20),
    seasonal_adjustment VARCHAR(50),
    is_preliminary BOOLEAN DEFAULT FALSE,
    is_revised BOOLEAN DEFAULT FALSE,
    category VARCHAR(100),
    source VARCHAR(50) DEFAULT 'fred',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(indicator_code, date, COALESCE(seasonal_adjustment, 'NSA'))
);

-- ============================================================================
-- 6. WEATHER DATA TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS weather_data (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    location VARCHAR(100) NOT NULL,
    country VARCHAR(2),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    date DATE NOT NULL,
    hour SMALLINT,
    temperature_celsius NUMERIC(5,2),
    temperature_fahrenheit NUMERIC(5,2),
    humidity_percent NUMERIC(5,2),
    pressure_mb NUMERIC(7,2),
    wind_speed_kmh NUMERIC(6,2),
    wind_direction_degrees NUMERIC(5,1),
    precipitation_mm NUMERIC(8,2) DEFAULT 0,
    cloud_coverage_percent NUMERIC(5,2),
    weather_condition VARCHAR(50),
    source VARCHAR(50) DEFAULT 'openweather',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(location, date, COALESCE(hour, -1), source)
);

-- ============================================================================
-- 7. PIPELINE METADATA TABLE (Execution Tracking & Monitoring)
-- ============================================================================

CREATE TABLE IF NOT EXISTS pipeline_metadata (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL UNIQUE,
    pipeline_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    success_count INTEGER DEFAULT 0,
    failure_count INTEGER DEFAULT 0,
    warning_count INTEGER DEFAULT 0,
    records_extracted INTEGER DEFAULT 0,
    records_transformed INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    total_duration_seconds NUMERIC(10,2),
    parameters JSONB,
    data_source VARCHAR(100),
    environment VARCHAR(50),
    executor VARCHAR(100),
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- CREATE INDEXES FOR QUERY OPTIMIZATION
-- ============================================================================

-- Stock Prices Indexes
CREATE INDEX IF NOT EXISTS idx_stock_symbol_date ON stock_prices (symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_stock_date ON stock_prices (date DESC);
CREATE INDEX IF NOT EXISTS idx_stock_source ON stock_prices (source);

-- Forex Rates Indexes
CREATE INDEX IF NOT EXISTS idx_forex_pair_date ON forex_rates (from_currency, to_currency, date DESC);
CREATE INDEX IF NOT EXISTS idx_forex_date ON forex_rates (date DESC);
CREATE INDEX IF NOT EXISTS idx_forex_source ON forex_rates (source);

-- Forex MT5 Indexes
CREATE INDEX IF NOT EXISTS idx_forex_mt5_pair_time ON forex_mt5 (from_currency, to_currency, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_forex_mt5_timeframe ON forex_mt5 (timeframe);
CREATE INDEX IF NOT EXISTS idx_forex_mt5_broker ON forex_mt5 (broker);
CREATE INDEX IF NOT EXISTS idx_forex_mt5_volume ON forex_mt5 (volume DESC);

-- Crypto Prices Indexes
CREATE INDEX IF NOT EXISTS idx_crypto_symbol_time ON crypto_prices (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_crypto_exchange ON crypto_prices (exchange);

-- Economic Indicators Indexes
CREATE INDEX IF NOT EXISTS idx_economic_code_date ON economic_indicators (indicator_code, date DESC);
CREATE INDEX IF NOT EXISTS idx_economic_category ON economic_indicators (category);
CREATE INDEX IF NOT EXISTS idx_economic_preliminary ON economic_indicators (is_preliminary);

-- Weather Data Indexes
CREATE INDEX IF NOT EXISTS idx_weather_location_date ON weather_data (location, date DESC);
CREATE INDEX IF NOT EXISTS idx_weather_condition ON weather_data (weather_condition);
CREATE INDEX IF NOT EXISTS idx_weather_temp ON weather_data (temperature_celsius);

-- Pipeline Metadata Indexes
CREATE INDEX IF NOT EXISTS idx_pipeline_run_id ON pipeline_metadata (run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_name_status ON pipeline_metadata (pipeline_name, status);
CREATE INDEX IF NOT EXISTS idx_pipeline_started ON pipeline_metadata (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_environment ON pipeline_metadata (environment);

-- ============================================================================
-- UPDATE TRIGGER FUNCTION (for tables with updated_at)
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to stock_prices
DROP TRIGGER IF EXISTS update_stock_prices_updated_at ON stock_prices;
CREATE TRIGGER update_stock_prices_updated_at
    BEFORE UPDATE ON stock_prices
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply trigger to forex_rates
DROP TRIGGER IF EXISTS update_forex_rates_updated_at ON forex_rates;
CREATE TRIGGER update_forex_rates_updated_at
    BEFORE UPDATE ON forex_rates
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply trigger to forex_mt5
DROP TRIGGER IF EXISTS update_forex_mt5_updated_at ON forex_mt5;
CREATE TRIGGER update_forex_mt5_updated_at
    BEFORE UPDATE ON forex_mt5
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply trigger to economic_indicators
DROP TRIGGER IF EXISTS update_economic_indicators_updated_at ON economic_indicators;
CREATE TRIGGER update_economic_indicators_updated_at
    BEFORE UPDATE ON economic_indicators
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply trigger to weather_data
DROP TRIGGER IF EXISTS update_weather_data_updated_at ON weather_data;
CREATE TRIGGER update_weather_data_updated_at
    BEFORE UPDATE ON weather_data
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply trigger to pipeline_metadata
DROP TRIGGER IF EXISTS update_pipeline_metadata_updated_at ON pipeline_metadata;
CREATE TRIGGER update_pipeline_metadata_updated_at
    BEFORE UPDATE ON pipeline_metadata
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT ' All tables created successfully!' as message;
SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;
