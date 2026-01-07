-- ============================================================================
-- Financial ETL Pipeline - Economic Indicators Table
-- ============================================================================
-- Purpose: Store macroeconomic indicators from Federal Reserve Economic Data (FRED)
-- Includes: GDP, Unemployment, Inflation, Interest Rates, Housing, etc.
-- Table Name: public.economic_indicators
-- Created: 2026-01-07
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.economic_indicators (
    -- ========================================================================
    -- Primary Key & Identifiers
    -- ========================================================================
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    indicator_code VARCHAR(50) NOT NULL COMMENT 'FRED indicator code (e.g., UNRATE, CPIAUCSL, GDP)',
    indicator_name VARCHAR(255) NOT NULL COMMENT 'Human-readable indicator name (e.g., Unemployment Rate)',
    
    -- ========================================================================
    -- Time Series Data
    -- ========================================================================
    date DATE NOT NULL COMMENT 'Observation date (monthly, quarterly, or annual)',
    value NUMERIC(15, 6) NOT NULL COMMENT 'Indicator value (units depend on indicator)',
    
    -- ========================================================================
    -- Metadata: Units & Frequency
    -- ========================================================================
    unit VARCHAR(100) COMMENT 'Units of measurement (%, points, billions, rate, index, etc)',
    frequency VARCHAR(20) COMMENT 'Data frequency: monthly, quarterly, annual',
    seasonal_adjustment VARCHAR(50) COMMENT 'Seasonal adjustment: NSA (not seasonally adjusted), SA (seasonally adjusted), etc',
    
    -- ========================================================================
    -- Data Quality Flags
    -- ========================================================================
    is_preliminary BOOLEAN DEFAULT FALSE COMMENT 'TRUE if data is preliminary and may be revised',
    is_revised BOOLEAN DEFAULT FALSE COMMENT 'TRUE if this is a revised value (previous release was different)',
    revision_number SMALLINT DEFAULT 0 COMMENT 'Number of revisions made to this value',
    
    -- ========================================================================
    -- Category & Classification
    -- ========================================================================
    category VARCHAR(100) COMMENT 'Data category: Employment, Inflation, GDP, Interest Rates, Housing, Monetary Policy, Trade, etc',
    region VARCHAR(50) COMMENT 'Geographic region or country: US, State, Country, or NULL for US National',
    
    -- ========================================================================
    -- Historical Context
    -- ========================================================================
    period_start DATE COMMENT 'Start date of observation period (for aggregations)',
    period_end DATE COMMENT 'End date of observation period (for aggregations)',
    
    -- ========================================================================
    -- Metadata
    -- ========================================================================
    source VARCHAR(50) NOT NULL DEFAULT 'fred' COMMENT 'Data source (fred, bis, ecb, world_bank, etc)',
    data_release_date DATE COMMENT 'Official release date from source (for tracking surprises)',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- ========================================================================
    -- Constraints: Data Validity
    -- ========================================================================
    CONSTRAINT chk_date_order CHECK (
        period_start IS NULL OR period_end IS NULL OR period_start <= period_end
    ),
    CONSTRAINT chk_revision_non_negative CHECK (revision_number >= 0),
    CONSTRAINT chk_indicator_code_not_empty CHECK (
        indicator_code ~ '^[A-Z0-9]{1,50}$'
    ),
    
    -- ========================================================================
    -- Constraint: Ensure unique indicator-date combination
    -- Critical for upsert operations and preventing duplicates
    -- ========================================================================
    UNIQUE(indicator_code, date, COALESCE(seasonal_adjustment, 'NSA'), COALESCE(region, 'US'))
);

-- ============================================================================
-- INDEXES: Query Optimization
-- ============================================================================

-- Index 1: Indicator lookup (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_economic_indicators_code 
    ON public.economic_indicators(indicator_code);

-- Index 2: Date range queries
CREATE INDEX IF NOT EXISTS idx_economic_indicators_date 
    ON public.economic_indicators(date DESC);

-- Index 3: Composite index for indicator + date range queries
-- Useful for: "Get all unemployment rates for the last 10 years"
CREATE INDEX IF NOT EXISTS idx_economic_indicators_code_date 
    ON public.economic_indicators(indicator_code, date DESC);

-- Index 4: Category-based searches (all employment data, all inflation data, etc)
CREATE INDEX IF NOT EXISTS idx_economic_indicators_category 
    ON public.economic_indicators(category);

-- Index 5: Creation timestamp (for tracking recent inserts/updates)
CREATE INDEX IF NOT EXISTS idx_economic_indicators_created_at 
    ON public.economic_indicators(created_at DESC);

-- Index 6: Upsert optimization (composite key for conflict detection)
CREATE UNIQUE INDEX IF NOT EXISTS idx_economic_indicators_upsert 
    ON public.economic_indicators(
        indicator_code, 
        date, 
        COALESCE(seasonal_adjustment, 'NSA'),
        COALESCE(region, 'US')
    );

-- Index 7: Preliminary data tracking (useful for forecasting)
CREATE INDEX IF NOT EXISTS idx_economic_indicators_preliminary 
    ON public.economic_indicators(is_preliminary) 
    WHERE is_preliminary = TRUE;

-- Index 8: Release date tracking (for event-driven analysis)
CREATE INDEX IF NOT EXISTS idx_economic_indicators_release_date 
    ON public.economic_indicators(data_release_date) 
    WHERE data_release_date IS NOT NULL;

-- ============================================================================
-- TRIGGER: Automatic Timestamp Management
-- ============================================================================

-- Create trigger function if it doesn't exist
CREATE OR REPLACE FUNCTION update_economic_indicators_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop trigger if exists (to avoid conflicts)
DROP TRIGGER IF EXISTS trg_economic_indicators_update_timestamp ON public.economic_indicators;

-- Create trigger
CREATE TRIGGER trg_economic_indicators_update_timestamp
BEFORE UPDATE ON public.economic_indicators
FOR EACH ROW
EXECUTE FUNCTION update_economic_indicators_timestamp();

-- ============================================================================
-- COMMENTS: Self-documenting Schema
-- ============================================================================

COMMENT ON TABLE public.economic_indicators IS 
'Macroeconomic indicators from Federal Reserve Economic Data (FRED) and other sources.
Used for fundamental analysis and macro trading strategies.
Includes employment (UNRATE, NONFARM), inflation (CPIAUCSL, CPILFESL), 
GDP, interest rates (FEDFUNDS, DGS10), housing (HOUST, MORTGAGE30US), and more.
Tracks preliminary vs. final data and revisions for accurate backtesting.
Updated via ETL pipeline with conflict resolution for multiple reporting methodologies.
Supports seasonal adjustment tracking for accurate comparisons.';

COMMENT ON COLUMN public.economic_indicators.id IS
'Unique identifier (UUID). Primary key for record-level operations.';

COMMENT ON COLUMN public.economic_indicators.indicator_code IS
'FRED or other official indicator code. Examples:
- UNRATE: Unemployment Rate (%)
- CPIAUCSL: Consumer Price Index
- PAYEMS: Total Nonfarm Payroll
- DCOILWTICO: WTI Crude Oil Price
- HOUST: Housing Starts
- MORTGAGE30US: 30-Year Mortgage Rate
Combined with date to prevent duplicates.';

COMMENT ON COLUMN public.economic_indicators.indicator_name IS
'Human-readable indicator name: "Unemployment Rate", "Consumer Price Index", etc.
Used for display and quick identification without code reference.';

COMMENT ON COLUMN public.economic_indicators.date IS
'Observation date in YYYY-MM-DD format. For monthly data, typically first day of month.
For quarterly, typically first day of quarter. For annual, typically first day of year.
Combined with indicator_code, seasonal adjustment, and region for uniqueness.';

COMMENT ON COLUMN public.economic_indicators.value IS
'Numeric value of the indicator. Units vary (see "unit" column).
Examples: 3.7 (for 3.7% unemployment), 308.5 (for CPI index), 1.234 (for interest rate %).';

COMMENT ON COLUMN public.economic_indicators.unit IS
'Units of measurement: "%" (percent), "index" (base 100), "billions", "rate", "index pts", etc.
Critical for correct interpretation and cross-source comparisons.';

COMMENT ON COLUMN public.economic_indicators.frequency IS
'Reporting frequency: "monthly", "quarterly", "annual", "weekly", "daily".
Affects how data points should be aggregated or interpolated.';

COMMENT ON COLUMN public.economic_indicators.seasonal_adjustment IS
'Seasonal adjustment methodology: "SA" (seasonally adjusted), "NSA" (not seasonally adjusted).
Important because same date can have different values depending on methodology.
Example: Jan employment usually low due to seasonality, SA removes this effect.';

COMMENT ON COLUMN public.economic_indicators.is_preliminary IS
'TRUE if data is preliminary and subject to future revisions.
Initial releases are often revised 1-2 months later with more complete data.
Important for accurate backtesting and avoiding forward-looking bias.';

COMMENT ON COLUMN public.economic_indicators.is_revised IS
'TRUE if this is a revision of a previously published value.
Helps identify data surprises where economists'' forecasts were wrong.
Example: Employment report shows lower numbers than previously estimated.';

COMMENT ON COLUMN public.economic_indicators.revision_number IS
'Count of revisions made to this specific data point.
Higher numbers indicate data that went through multiple estimation rounds.
Zero = first release, 1 = first revision, 2 = second revision, etc.';

COMMENT ON COLUMN public.economic_indicators.category IS
'Logical grouping: "Employment", "Inflation", "GDP", "Interest Rates", 
"Housing", "Consumer Spending", "Manufacturing", "International Trade", "Monetary Policy".
Used for thematic analysis and filtering related indicators.';

COMMENT ON COLUMN public.economic_indicators.region IS
'Geographic scope: NULL or "US" for national data, state codes (CA, NY, TX) for states,
or country codes (DE, FR, JP) for international data.
Allows subsetting analysis to specific regions.';

COMMENT ON COLUMN public.economic_indicators.period_start IS
'Start date of the observation period for aggregated data.
Example: For Jan employment, period_start = 2026-01-01, period_end = 2026-01-31.
NULL for non-aggregated point-in-time data.';

COMMENT ON COLUMN public.economic_indicators.period_end IS
'End date of the observation period for aggregated data.
Used to calculate duration and overlap checks with other indicators.
NULL for non-aggregated point-in-time data.';

COMMENT ON COLUMN public.economic_indicators.source IS
'Data origin: "fred" (Federal Reserve), "bis" (Bank for International Settlements),
"ecb" (European Central Bank), "world_bank", "istat" (Italy), "insee" (France), etc.
Allows tracking data lineage and handling source-specific quirks.';

COMMENT ON COLUMN public.economic_indicators.data_release_date IS
'Official announcement date from source (when data was first published).
Used for event-driven analysis and identifying market surprises.
Example: Employment report released first Friday of month.';

COMMENT ON COLUMN public.economic_indicators.created_at IS
'Timestamp when record was first inserted into our database.
Set once at creation, never modified. Used for audit trail.';

COMMENT ON COLUMN public.economic_indicators.updated_at IS
'Timestamp of last modification. Automatically updated by trigger.
Used to identify stale data and track when revisions were recorded.';

-- ============================================================================
-- COMMON FRED INDICATORS (Examples)
-- ============================================================================

-- Employment Indicators:
-- UNRATE: Unemployment Rate (Monthly, %)
-- PAYEMS: Total Nonfarm Payroll (Monthly, Thousands)
-- CIVPART: Labor Force Participation Rate (Monthly, %)
-- ICSA: Initial Claims (Weekly, Thousands)

-- Inflation Indicators:
-- CPIAUCSL: Consumer Price Index, All Urban Consumers (Monthly, Index)
-- CPILFESL: Consumer Price Index excluding Food and Energy (Monthly, Index)
-- PCEPI: Personal Consumption Expenditures Price Index (Monthly, Index)

-- GDP & Growth:
-- A191RL1Q225SBEA: Real Gross Domestic Product (Quarterly, Billions)
-- A191RA1Q225SBEA: Gross Domestic Product (Quarterly, Billions)

-- Interest Rates:
-- FEDFUNDS: Effective Federal Funds Rate (Monthly, %)
-- DGS10: 10-Year Treasury Constant Maturity Rate (Daily, %)
-- DGS2: 2-Year Treasury Constant Maturity Rate (Daily, %)
-- MORTGAGE30US: 30-Year Fixed Rate Mortgage Average (Weekly, %)

-- Housing:
-- HOUST: Total Housing Starts (Monthly, Thousands)
-- HOUST1F: Single-Family Housing Starts (Monthly, Thousands)
-- MORTGAGE30US: Mortgage Rate (Weekly, %)

-- Consumer Spending:
-- RSXFS: Retail Sales, Excluding Food Services (Monthly, Millions)
-- DAUTOSXM: Manufacturers'' New Orders, Durable Goods (Monthly, Millions)

-- ============================================================================
-- EXAMPLE DATA
-- ============================================================================

-- Example 1: Insert unemployment rate data
-- INSERT INTO public.economic_indicators (
--     indicator_code, indicator_name, date, value,
--     unit, frequency, seasonal_adjustment, category
-- ) VALUES (
--     'UNRATE', 'Unemployment Rate', '2026-01-01', 3.7,
--     '%', 'monthly', 'SA', 'Employment'
-- );

-- Example 2: Bulk insert multiple indicators
-- INSERT INTO public.economic_indicators (
--     indicator_code, indicator_name, date, value,
--     unit, frequency, category
-- ) VALUES
--     ('UNRATE', 'Unemployment Rate', '2026-01-01', 3.7, '%', 'monthly', 'Employment'),
--     ('PAYEMS', 'Total Nonfarm Payroll', '2026-01-01', 156850, 'thousands', 'monthly', 'Employment'),
--     ('CPIAUCSL', 'Consumer Price Index', '2026-01-01', 321.5, 'index', 'monthly', 'Inflation'),
--     ('FEDFUNDS', 'Fed Funds Rate', '2026-01-01', 4.33, '%', 'monthly', 'Interest Rates');

-- Example 3: Upsert pattern (used by Python ETL)
-- INSERT INTO public.economic_indicators (
--     indicator_code, indicator_name, date, value, unit, 
--     frequency, seasonal_adjustment, category, source
-- ) VALUES (
--     'UNRATE', 'Unemployment Rate', '2026-01-01', 3.7,
--     '%', 'monthly', 'SA', 'Employment', 'fred'
-- ) ON CONFLICT (
--     indicator_code, date, 
--     COALESCE(seasonal_adjustment, 'NSA'), 
--     COALESCE(region, 'US')
-- ) DO UPDATE SET
--     value = EXCLUDED.value,
--     is_revised = TRUE,
--     revision_number = economic_indicators.revision_number + 1,
--     updated_at = CURRENT_TIMESTAMP;

-- ============================================================================
-- COMMON QUERIES
-- ============================================================================

-- Query 1: Get latest unemployment rate
-- SELECT date, value as unemployment_rate
-- FROM public.economic_indicators 
-- WHERE indicator_code = 'UNRATE'
-- ORDER BY date DESC LIMIT 1;

-- Query 2: Get 12-month unemployment trend
-- SELECT date, value,
--   LAG(value) OVER (ORDER BY date) as prev_month,
--   value - LAG(value) OVER (ORDER BY date) as change_pts
-- FROM public.economic_indicators 
-- WHERE indicator_code = 'UNRATE' 
--   AND date >= CURRENT_DATE - INTERVAL '12 months'
-- ORDER BY date DESC;

-- Query 3: Get revised vs preliminary data for recent release
-- SELECT indicator_code, indicator_name, date, value, 
--        is_preliminary, revision_number
-- FROM public.economic_indicators 
-- WHERE data_release_date = CURRENT_DATE - INTERVAL '1 day'
-- ORDER BY indicator_code;

-- Query 4: Compare inflation indicators
-- SELECT date,
--   (SELECT value FROM public.economic_indicators 
--    WHERE indicator_code = 'CPIAUCSL' AND ei.date = date) as cpi,
--   (SELECT value FROM public.economic_indicators 
--    WHERE indicator_code = 'CPILFESL' AND ei.date = date) as cpi_core,
--   (SELECT value FROM public.economic_indicators 
--    WHERE indicator_code = 'PCEPI' AND ei.date = date) as pce
-- FROM public.economic_indicators ei
-- WHERE date >= CURRENT_DATE - INTERVAL '24 months'
-- GROUP BY date
-- ORDER BY date DESC;
