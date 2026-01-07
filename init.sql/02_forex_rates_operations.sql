-- ============================================================================
-- Forex Rates - Insert and Upsert Examples
-- ============================================================================
-- Purpose: Demonstrate how to insert and update forex rate data
-- ============================================================================

-- ============================================================================
-- INSERT: Single Record Example
-- ============================================================================
INSERT INTO public.forex_rates (
    from_currency,
    to_currency,
    date,
    open,
    high,
    low,
    close,
    source
) VALUES (
    'EUR',
    'USD',
    '2026-01-07',
    1.0852,
    1.0923,
    1.0821,
    1.0891,
    'alpha_vantage'
);

-- ============================================================================
-- UPSERT: Insert or Update if Duplicate Key
-- ============================================================================
-- This is the pattern used by the Python ETL pipeline
INSERT INTO public.forex_rates (
    from_currency,
    to_currency,
    date,
    open,
    high,
    low,
    close,
    source
) VALUES (
    'EUR',
    'USD',
    '2026-01-07',
    1.0852,
    1.0923,
    1.0821,
    1.0891,
    'alpha_vantage'
)
ON CONFLICT (from_currency, to_currency, date)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    source = EXCLUDED.source,
    updated_at = CURRENT_TIMESTAMP;

-- ============================================================================
-- BULK INSERT: Multiple Records
-- ============================================================================
INSERT INTO public.forex_rates (
    from_currency,
    to_currency,
    date,
    open,
    high,
    low,
    close
) VALUES
    ('EUR', 'USD', '2026-01-07', 1.0852, 1.0923, 1.0821, 1.0891),
    ('EUR', 'USD', '2026-01-06', 1.0821, 1.0845, 1.0795, 1.0852),
    ('GBP', 'USD', '2026-01-07', 1.2650, 1.2720, 1.2580, 1.2695),
    ('GBP', 'USD', '2026-01-06', 1.2620, 1.2670, 1.2595, 1.2650),
    ('USD', 'JPY', '2026-01-07', 145.50, 146.20, 145.30, 145.95),
    ('USD', 'JPY', '2026-01-06', 145.80, 146.10, 145.40, 145.50)
ON CONFLICT (from_currency, to_currency, date)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    updated_at = CURRENT_TIMESTAMP;

-- ============================================================================
-- SELECT: Query Examples
-- ============================================================================

-- Get latest rate for a specific pair
SELECT 
    from_currency || '/' || to_currency AS pair,
    date,
    open,
    high,
    low,
    close,
    created_at
FROM public.forex_rates
WHERE from_currency = 'EUR' AND to_currency = 'USD'
ORDER BY date DESC
LIMIT 1;

-- Get price range for a pair over a date range
SELECT 
    from_currency || '/' || to_currency AS pair,
    date,
    MIN(low) AS min_rate,
    MAX(high) AS max_rate,
    AVG(close) AS avg_rate
FROM public.forex_rates
WHERE from_currency = 'EUR' 
    AND to_currency = 'USD'
    AND date BETWEEN '2026-01-01' AND '2026-01-07'
GROUP BY from_currency, to_currency, date
ORDER BY date DESC;

-- Get volatility statistics
SELECT 
    from_currency || '/' || to_currency AS pair,
    COUNT(*) AS num_days,
    AVG(close) AS avg_close,
    MIN(low) AS min_low,
    MAX(high) AS max_high,
    ROUND(STDDEV(close)::NUMERIC, 6) AS std_dev,
    ROUND((MAX(high) - MIN(low)) / AVG(close) * 100, 2) AS range_pct
FROM public.forex_rates
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY from_currency, to_currency
ORDER BY std_dev DESC;

-- Get all available currency pairs with latest rates
SELECT DISTINCT
    fr.from_currency || '/' || fr.to_currency AS pair,
    fr.date,
    fr.close AS latest_rate,
    LAG(fr.close) OVER (PARTITION BY fr.from_currency, fr.to_currency ORDER BY fr.date) AS prev_close,
    ROUND(
        (fr.close - LAG(fr.close) OVER (PARTITION BY fr.from_currency, fr.to_currency ORDER BY fr.date))
        / LAG(fr.close) OVER (PARTITION BY fr.from_currency, fr.to_currency ORDER BY fr.date) * 100,
        4
    ) AS change_pct
FROM public.forex_rates fr
WHERE (fr.from_currency, fr.to_currency, fr.date) IN (
    SELECT from_currency, to_currency, MAX(date)
    FROM public.forex_rates
    GROUP BY from_currency, to_currency
)
ORDER BY pair;

-- ============================================================================
-- ANALYSIS: Advanced Queries
-- ============================================================================

-- Day-over-day percentage change
SELECT 
    from_currency || '/' || to_currency AS pair,
    date,
    close,
    LAG(close) OVER (PARTITION BY from_currency, to_currency ORDER BY date) AS prev_close,
    ROUND(
        (close - LAG(close) OVER (PARTITION BY from_currency, to_currency ORDER BY date))
        / LAG(close) OVER (PARTITION BY from_currency, to_currency ORDER BY date) * 100,
        4
    ) AS change_pct
FROM public.forex_rates
WHERE from_currency = 'EUR' AND to_currency = 'USD'
    AND date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY date DESC;

-- 7-day and 30-day moving averages
SELECT 
    date,
    close,
    ROUND(
        AVG(close) OVER (
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC,
        6
    ) AS ma_7d,
    ROUND(
        AVG(close) OVER (
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC,
        6
    ) AS ma_30d
FROM public.forex_rates
WHERE from_currency = 'EUR' AND to_currency = 'USD'
    AND date >= CURRENT_DATE - INTERVAL '60 days'
ORDER BY date DESC;

-- High volatility detection (>2% daily movement)
SELECT 
    from_currency || '/' || to_currency AS pair,
    date,
    open,
    high,
    low,
    close,
    ROUND(((high - low) / low * 100)::NUMERIC, 2) AS volatility_pct,
    CASE 
        WHEN (high - low) / low > 0.05 THEN 'Very High'
        WHEN (high - low) / low > 0.03 THEN 'High'
        WHEN (high - low) / low > 0.02 THEN 'Medium'
        ELSE 'Low'
    END AS volatility_level
FROM public.forex_rates
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY volatility_pct DESC;

-- ============================================================================
-- MAINTENANCE: Cleanup and Optimization
-- ============================================================================

-- Count total records
SELECT COUNT(*) AS total_records FROM public.forex_rates;

-- Count records per currency pair
SELECT 
    from_currency || '/' || to_currency AS pair,
    COUNT(*) AS num_records,
    MIN(date) AS earliest_date,
    MAX(date) AS latest_date
FROM public.forex_rates
GROUP BY from_currency, to_currency
ORDER BY pair;

-- Find gaps in data (missing dates for a pair)
-- For daily data, should have records for each business day
SELECT 
    from_currency,
    to_currency,
    date + INTERVAL '1 day' AS missing_date
FROM public.forex_rates fr1
WHERE NOT EXISTS (
    SELECT 1 FROM public.forex_rates fr2
    WHERE fr2.from_currency = fr1.from_currency
        AND fr2.to_currency = fr1.to_currency
        AND fr2.date = fr1.date + INTERVAL '1 day'
)
    AND fr1.date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY from_currency, to_currency, date;

-- Identify duplicate or near-duplicate records
SELECT 
    from_currency,
    to_currency,
    date,
    COUNT(*) AS cnt
FROM public.forex_rates
GROUP BY from_currency, to_currency, date
HAVING COUNT(*) > 1;

-- Vacuum and analyze for performance
VACUUM ANALYZE public.forex_rates;

-- ============================================================================
-- CLEANUP: Delete old records (optional, be careful!)
-- ============================================================================

-- Delete records older than 2 years
-- DELETE FROM public.forex_rates 
-- WHERE date < CURRENT_DATE - INTERVAL '2 years';

-- Delete a specific pair (example: old test data)
-- DELETE FROM public.forex_rates
-- WHERE from_currency = 'XXX' AND to_currency = 'YYY';

-- ============================================================================
-- GRANT: Access Control (if using roles)
-- ============================================================================

-- Grant read access to a role
-- GRANT SELECT ON public.forex_rates TO etl_reader;

-- Grant write access to a role  
-- GRANT INSERT, UPDATE, DELETE ON public.forex_rates TO etl_writer;

-- Grant all access
-- GRANT ALL ON public.forex_rates TO etl_admin;
