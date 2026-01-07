-- ============================================================================
-- Financial ETL Pipeline - Weather Data Table
-- ============================================================================
-- Purpose: Store historical weather data for financial analysis
-- (Weather affects energy prices, agricultural commodities, etc)
-- Table Name: public.weather_data
-- Created: 2026-01-07
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.weather_data (
    -- ========================================================================
    -- Primary Key & Location
    -- ========================================================================
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    city VARCHAR(100) NOT NULL COMMENT 'City name (e.g., New York, London, Tokyo)',
    country VARCHAR(2) COMMENT 'ISO 3166-1 alpha-2 country code (e.g., US, GB, JP)',
    latitude NUMERIC(9, 6) COMMENT 'Geographic latitude (-90 to 90)',
    longitude NUMERIC(9, 6) COMMENT 'Geographic longitude (-180 to 180)',
    
    -- ========================================================================
    -- Time Data
    -- ========================================================================
    date DATE NOT NULL COMMENT 'Date of weather observation',
    hour SMALLINT COMMENT 'Hour of day (0-23) for hourly data; NULL for daily summary',
    
    -- ========================================================================
    -- Temperature Data
    -- ========================================================================
    temperature_celsius NUMERIC(5, 2) COMMENT 'Current/Average temperature in Celsius',
    temperature_fahrenheit NUMERIC(5, 2) COMMENT 'Current/Average temperature in Fahrenheit',
    feels_like_celsius NUMERIC(5, 2) COMMENT 'Apparent/feels-like temperature in Celsius',
    min_temperature_celsius NUMERIC(5, 2) COMMENT 'Daily minimum temperature in Celsius',
    max_temperature_celsius NUMERIC(5, 2) COMMENT 'Daily maximum temperature in Celsius',
    dew_point_celsius NUMERIC(5, 2) COMMENT 'Dew point temperature in Celsius',
    
    -- ========================================================================
    -- Atmospheric Data
    -- ========================================================================
    humidity_percent NUMERIC(5, 2) COMMENT 'Relative humidity (0-100%)',
    pressure_mb NUMERIC(7, 2) COMMENT 'Atmospheric pressure in millibars',
    pressure_in NUMERIC(6, 3) COMMENT 'Atmospheric pressure in inches of mercury',
    
    -- ========================================================================
    -- Wind Data
    -- ========================================================================
    wind_speed_kmh NUMERIC(6, 2) COMMENT 'Wind speed in kilometers per hour',
    wind_speed_mph NUMERIC(6, 2) COMMENT 'Wind speed in miles per hour',
    wind_speed_ms NUMERIC(6, 2) COMMENT 'Wind speed in meters per second',
    wind_direction_degrees NUMERIC(5, 1) COMMENT 'Wind direction in degrees (0-360, N=0)',
    wind_direction_cardinal VARCHAR(3) COMMENT 'Wind direction as cardinal (N, NE, E, etc)',
    wind_gust_kmh NUMERIC(6, 2) COMMENT 'Wind gust speed in kilometers per hour',
    
    -- ========================================================================
    -- Precipitation & Clouds
    -- ========================================================================
    precipitation_mm NUMERIC(8, 2) DEFAULT 0 COMMENT 'Rainfall in millimeters',
    precipitation_in NUMERIC(8, 3) DEFAULT 0 COMMENT 'Rainfall in inches',
    snow_mm NUMERIC(8, 2) COMMENT 'Snowfall in millimeters',
    cloud_coverage_percent NUMERIC(5, 2) COMMENT 'Cloud coverage (0-100%)',
    visibility_km NUMERIC(8, 2) COMMENT 'Visibility in kilometers',
    visibility_miles NUMERIC(8, 2) COMMENT 'Visibility in miles',
    
    -- ========================================================================
    -- Weather Conditions
    -- ========================================================================
    weather_condition VARCHAR(50) COMMENT 'Main weather condition (Clear, Cloudy, Rainy, Snowy, etc)',
    weather_description VARCHAR(255) COMMENT 'Detailed weather description',
    uv_index NUMERIC(3, 1) COMMENT 'UV index (0-20+ scale)',
    
    -- ========================================================================
    -- Metadata
    -- ========================================================================
    source VARCHAR(50) NOT NULL DEFAULT 'openweather' COMMENT 'Data source (openweather, etc)',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- ========================================================================
    -- Constraints: Data Validity
    -- ========================================================================
    CONSTRAINT chk_humidity_range CHECK (humidity_percent >= 0 AND humidity_percent <= 100),
    CONSTRAINT chk_cloud_coverage_range CHECK (cloud_coverage_percent >= 0 AND cloud_coverage_percent <= 100),
    CONSTRAINT chk_wind_direction_range CHECK (wind_direction_degrees >= 0 AND wind_direction_degrees <= 360),
    CONSTRAINT chk_hour_range CHECK (hour IS NULL OR (hour >= 0 AND hour <= 23)),
    CONSTRAINT chk_latitude_range CHECK (latitude >= -90 AND latitude <= 90),
    CONSTRAINT chk_longitude_range CHECK (longitude >= -180 AND longitude <= 180),
    CONSTRAINT chk_precipitation_non_negative CHECK (precipitation_mm >= 0 AND precipitation_in >= 0),
    CONSTRAINT chk_wind_speed_non_negative CHECK (
        wind_speed_kmh >= 0 AND wind_speed_mph >= 0 AND wind_speed_ms >= 0
    ),
    CONSTRAINT chk_uv_index_non_negative CHECK (uv_index >= 0),
    
    -- ========================================================================
    -- Constraint: Ensure unique city-date-hour combination
    -- Critical for upsert operations (prevent duplicates)
    -- ========================================================================
    UNIQUE(city, date, COALESCE(hour, -1))
);

-- ============================================================================
-- INDEXES: Query Optimization
-- ============================================================================

-- Index 1: City lookup (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_weather_data_city 
    ON public.weather_data(city);

-- Index 2: Date range queries
CREATE INDEX IF NOT EXISTS idx_weather_data_date 
    ON public.weather_data(date DESC);

-- Index 3: Composite index for city + date range queries
-- Useful for: "Get all weather data for New York between Jan 1 and Dec 31"
CREATE INDEX IF NOT EXISTS idx_weather_data_city_date 
    ON public.weather_data(city, date DESC);

-- Index 4: Temporal queries (recent weather)
CREATE INDEX IF NOT EXISTS idx_weather_data_created_at 
    ON public.weather_data(created_at DESC);

-- Index 5: Upsert optimization (city-date-hour composite for conflict detection)
CREATE UNIQUE INDEX IF NOT EXISTS idx_weather_data_upsert 
    ON public.weather_data(city, date, COALESCE(hour, -1));

-- Index 6: Weather condition searches (analytical queries)
CREATE INDEX IF NOT EXISTS idx_weather_data_condition 
    ON public.weather_data(weather_condition);

-- Index 7: Temperature range queries (heat waves, cold snaps)
CREATE INDEX IF NOT EXISTS idx_weather_data_temperature 
    ON public.weather_data(max_temperature_celsius, min_temperature_celsius);

-- ============================================================================
-- TRIGGER: Automatic Timestamp Management
-- ============================================================================

-- Create trigger function if it doesn't exist
CREATE OR REPLACE FUNCTION update_weather_data_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop trigger if exists (to avoid conflicts)
DROP TRIGGER IF EXISTS trg_weather_data_update_timestamp ON public.weather_data;

-- Create trigger
CREATE TRIGGER trg_weather_data_update_timestamp
BEFORE UPDATE ON public.weather_data
FOR EACH ROW
EXECUTE FUNCTION update_weather_data_timestamp();

-- ============================================================================
-- COMMENTS: Self-documenting Schema
-- ============================================================================

COMMENT ON TABLE public.weather_data IS 
'Historical daily and hourly weather data for major financial centers.
Useful for analyzing correlation between weather and asset prices.
Examples: Energy prices (weather affects supply/demand), Agricultural commodities,
Retail sales (weather affects consumer behavior), Airline stocks (storms impact operations).
Stores temperature, wind, precipitation, and other meteorological measurements.
Updated via ETL pipeline with conflict resolution using city-date-hour composite key.
Indexes optimized for time-series queries and rapid geographic lookups.';

COMMENT ON COLUMN public.weather_data.id IS
'Unique identifier (UUID). Primary key for record-level operations.';

COMMENT ON COLUMN public.weather_data.city IS
'City name where weather was observed. Combined with date/hour for unique constraint.
Examples: New York, London, Tokyo, São Paulo, Hong Kong.';

COMMENT ON COLUMN public.weather_data.country IS
'ISO 3166-1 alpha-2 country code. Helps disambiguate cities (e.g., Portland, OR vs Portland, ME).
Examples: US, GB, JP, BR, HK, CN, IN.';

COMMENT ON COLUMN public.weather_data.latitude IS
'Geographic latitude in decimal degrees (-90 to +90). Used for regional aggregations.
Positive = North, Negative = South.';

COMMENT ON COLUMN public.weather_data.longitude IS
'Geographic longitude in decimal degrees (-180 to +180). Used for regional aggregations.
Positive = East, Negative = West.';

COMMENT ON COLUMN public.weather_data.date IS
'Observation date in YYYY-MM-DD format. Stored in UTC for consistency.
Combined with city and hour to ensure unique records.';

COMMENT ON COLUMN public.weather_data.hour IS
'Hour of day (0-23) for hourly data. NULL indicates daily summary/aggregate data.
Uses 24-hour UTC format (0=midnight, 12=noon, 23=11 PM).';

COMMENT ON COLUMN public.weather_data.temperature_celsius IS
'Current or average temperature in degrees Celsius.
Used in calculations and comparisons with historical averages.';

COMMENT ON COLUMN public.weather_data.feels_like_celsius IS
'Apparent temperature accounting for wind chill or heat index.
More relevant for human comfort and behavioral analysis than actual temperature.';

COMMENT ON COLUMN public.weather_data.humidity_percent IS
'Relative humidity as percentage (0-100%). Higher values indicate more moisture.
Impacts energy demand (heating/cooling), crop health, and infrastructure stress.';

COMMENT ON COLUMN public.weather_data.pressure_mb IS
'Atmospheric pressure in millibars (typically 980-1050 mb).
Low pressure systems often bring storms; high pressure brings clear weather.
Used for weather pattern analysis and meteorological forecasting.';

COMMENT ON COLUMN public.weather_data.wind_speed_kmh IS
'Wind speed in kilometers per hour. Key factor in energy (wind power) and transportation.
Impacts airline operations, shipping, and construction industries.';

COMMENT ON COLUMN public.weather_data.wind_direction_degrees IS
'Wind direction as degrees from North (0° = N, 90° = E, 180° = S, 270° = W).
Used with wind speed to determine meteorological direction patterns.';

COMMENT ON COLUMN public.weather_data.precipitation_mm IS
'Rainfall amount in millimeters. Critical for agricultural commodities and water management.
Zero = dry conditions, higher values = wetter conditions and flood risk.';

COMMENT ON COLUMN public.weather_data.snow_mm IS
'Snowfall amount in millimeters. Relevant for winter energy demand and transportation disruptions.
NULL indicates no snow or data not available.';

COMMENT ON COLUMN public.weather_data.cloud_coverage_percent IS
'Percentage of sky covered by clouds (0-100%). 0% = clear, 100% = overcast.
Affects solar irradiance and renewable energy generation.';

COMMENT ON COLUMN public.weather_data.visibility_km IS
'Horizontal visibility in kilometers. Lower values indicate fog, haze, or poor conditions.
Impacts traffic flow, travel delays, and accident rates.';

COMMENT ON COLUMN public.weather_data.weather_condition IS
'Main weather condition category: Clear, Cloudy, Rainy, Snowy, Thunderstorm, Fog, etc.
Used for quick filtering and trend analysis of severe weather.';

COMMENT ON COLUMN public.weather_data.weather_description IS
'Detailed description: "Light rain", "Heavy thunderstorm", "Scattered clouds", etc.
Provides additional context beyond main condition category.';

COMMENT ON COLUMN public.weather_data.uv_index IS
'UV Index on standard 0-20+ scale. 0-2 (Low), 3-5 (Moderate), 6-7 (High), 8-10 (Very High), 11+ (Extreme).
Impacts retail (sunscreen/sunglasses sales), health (skin cancer rates), and tourism.';

COMMENT ON COLUMN public.weather_data.source IS
'Data source identifier for tracking lineage and handling conflicts.
Example: "openweather" (OpenWeather API), "weather_gov" (US National Weather Service).';

COMMENT ON COLUMN public.weather_data.created_at IS
'Timestamp when record was first inserted. Set once at creation, never modified.
Used for audit trail and identifying bulk import batches.';

COMMENT ON COLUMN public.weather_data.updated_at IS
'Timestamp of last modification. Automatically updated by trigger on any changes.
Used to identify stale records and track data refresh timing.';

-- ============================================================================
-- EXAMPLE DATA
-- ============================================================================

-- Example 1: Daily weather data
-- INSERT INTO public.weather_data (
--     city, country, date, 
--     temperature_celsius, humidity_percent, 
--     precipitation_mm, weather_condition
-- ) VALUES (
--     'New York', 'US', '2026-01-07',
--     2.5, 65,
--     0, 'Clear'
-- );

-- Example 2: Bulk insert with hourly data
-- INSERT INTO public.weather_data (
--     city, date, hour,
--     temperature_celsius, humidity_percent,
--     wind_speed_kmh, wind_direction_degrees,
--     precipitation_mm, cloud_coverage_percent
-- ) VALUES
--     ('London', '2026-01-07', 0, 4.2, 72, 12, 180, 0.5, 60),
--     ('London', '2026-01-07', 1, 3.9, 75, 11, 185, 0.3, 65),
--     ('London', '2026-01-07', 2, 3.5, 78, 10, 190, 0, 70);

-- Example 3: Upsert pattern (used by Python ETL)
-- INSERT INTO public.weather_data (
--     city, country, date, hour,
--     temperature_celsius, feels_like_celsius,
--     humidity_percent, precipitation_mm,
--     weather_condition, source
-- ) VALUES (
--     'Tokyo', 'JP', '2026-01-07', NULL,
--     5.8, 3.2,
--     52, 0,
--     'Clear', 'openweather'
-- ) ON CONFLICT (city, date, COALESCE(hour, -1))
-- DO UPDATE SET
--     temperature_celsius = EXCLUDED.temperature_celsius,
--     humidity_percent = EXCLUDED.humidity_percent,
--     precipitation_mm = EXCLUDED.precipitation_mm,
--     weather_condition = EXCLUDED.weather_condition,
--     updated_at = CURRENT_TIMESTAMP;

-- ============================================================================
-- COMMON QUERIES
-- ============================================================================

-- Query 1: Get latest weather for a city
-- SELECT date, hour, temperature_celsius, humidity_percent, weather_condition
-- FROM public.weather_data 
-- WHERE city = 'New York' AND date = CURRENT_DATE
-- ORDER BY hour DESC NULLS LAST;

-- Query 2: Extreme weather events (heat waves, cold snaps)
-- SELECT city, date, max_temperature_celsius, min_temperature_celsius
-- FROM public.weather_data 
-- WHERE date >= CURRENT_DATE - INTERVAL '30 days'
--   AND (max_temperature_celsius > 35 OR min_temperature_celsius < -10)
-- ORDER BY date DESC;

-- Query 3: Daily precipitation summary
-- SELECT city, date, 
--   SUM(precipitation_mm) as total_daily_rain,
--   COUNT(*) as hourly_observations
-- FROM public.weather_data 
-- WHERE date >= CURRENT_DATE - INTERVAL '7 days'
-- GROUP BY city, date
-- HAVING SUM(precipitation_mm) > 10
-- ORDER BY total_daily_rain DESC;

-- Query 4: Weather condition frequency
-- SELECT city, 
--   weather_condition, 
--   COUNT(*) as frequency,
--   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY city), 1) as pct
-- FROM public.weather_data 
-- WHERE date >= CURRENT_DATE - INTERVAL '30 days'
-- GROUP BY city, weather_condition
-- ORDER BY city, frequency DESC;
