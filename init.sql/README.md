# Financial ETL Pipeline - Database Schema

This directory contains all SQL schema definitions for the financial ETL pipeline database.

## Files Overview

### 1. `01_create_forex_rates.sql`
**Purpose:** Historical forex currency pair exchange rates  
**Records:** Daily OHLC (Open, High, Low, Close) data for currency pairs  
**Key Features:**
- Unique constraint on (from_currency, to_currency, date) for upsert operations
- 6 strategic indexes for optimal query performance
- Auto-updating timestamp trigger
- Comprehensive OHLC validation constraints
- ISO 4217 currency code validation

**Example Data:**
- EUR/USD rates since 2020
- GBP/USD, USD/JPY, USD/CHF pairs
- Daily frequency, stored in UTC

**Common Queries:**
```sql
-- Get latest EUR/USD rate
SELECT close FROM public.forex_rates 
WHERE from_currency = 'EUR' AND to_currency = 'USD'
ORDER BY date DESC LIMIT 1;

-- 30-day moving average
SELECT date, AVG(close) OVER (
  ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
) as ma_30
FROM public.forex_rates 
WHERE from_currency = 'EUR' AND to_currency = 'USD';
```

---

### 2. `02_forex_rates_operations.sql`
**Purpose:** Example SQL operations for forex_rates table  
**Contains:**
- INSERT examples (single and bulk)
- UPSERT patterns (used by Python ETL)
- SELECT queries (latest rates, moving averages, volatility)
- Analysis queries (day-over-day changes, volatility detection)
- Maintenance queries (data gaps, cleanup)
- Access control examples (GRANT statements)

**Use Case:** Reference guide for developers implementing forex-related features

---

### 3. `03_create_stock_prices.sql`
**Purpose:** Historical daily stock price data  
**Records:** OHLCV (Open, High, Low, Close, Volume) for equities  
**Key Features:**
- Unique constraint on (symbol, date) for upsert operations
- 6 strategic indexes (symbol, date, composite, creation time, upsert, source)
- OHLC validity constraints (high >= low >= open, close, etc.)
- Volume tracking and adjusted close for splits/dividends
- Source tracking (alpha_vantage, finnhub, yahoo_finance, etc.)

**Example Data:**
- AAPL, MSFT, GOOGL daily prices
- Volume data for technical analysis
- Adjusted close for dividend-adjusted returns

**Common Queries:**
```sql
-- 30-day moving average for AAPL
SELECT date, 
  AVG(close) OVER (
    ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) as ma_30
FROM public.stock_prices 
WHERE symbol = 'AAPL' AND date >= CURRENT_DATE - INTERVAL '90 days';

-- Daily returns
SELECT date, close,
  ((close - LAG(close) OVER (ORDER BY date)) / LAG(close) OVER (ORDER BY date)) * 100 as daily_return_pct
FROM public.stock_prices WHERE symbol = 'AAPL';
```

---

### 4. `04_create_weather_data.sql`
**Purpose:** Historical weather data for financial analysis  
**Records:** Daily and hourly weather observations for major financial centers  
**Key Features:**
- Unique constraint on (city, date, hour) for hourly and daily data
- 7 strategic indexes (city, date, condition, temperature, creation time)
- Comprehensive meteorological measurements (temp, humidity, wind, precipitation)
- Weather condition categorization
- Source tracking (openweather, weather_gov, etc.)

**Example Data:**
- New York, London, Tokyo, São Paulo, Hong Kong
- Temperature, humidity, wind speed, precipitation
- Cloud coverage, visibility, UV index
- Useful for energy prices, agricultural commodities, airline stocks

**Common Queries:**
```sql
-- Extreme weather events
SELECT city, date, max_temperature_celsius, min_temperature_celsius
FROM public.weather_data 
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
  AND (max_temperature_celsius > 35 OR min_temperature_celsius < -10);

-- Daily precipitation
SELECT city, date, SUM(precipitation_mm) as total_daily_rain
FROM public.weather_data WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY city, date HAVING SUM(precipitation_mm) > 10;
```

---

### 5. `05_create_economic_indicators.sql`
**Purpose:** Macroeconomic indicators from FRED and other sources  
**Records:** Monthly/quarterly/annual economic data (unemployment, inflation, GDP, etc.)  
**Key Features:**
- Unique constraint on (indicator_code, date, seasonal_adjustment, region)
- 8 strategic indexes (code, date, category, release date, preliminary data)
- Tracks preliminary vs. final data and revisions
- Supports seasonal adjustment tracking
- Data release date for event-driven analysis

**Example Data:**
- UNRATE (Unemployment Rate)
- CPIAUCSL (Consumer Price Index)
- PAYEMS (Nonfarm Payroll)
- FEDFUNDS (Fed Funds Rate)
- HOUST (Housing Starts)
- DGS10 (10-Year Treasury Rate)

**Common Queries:**
```sql
-- Unemployment trend
SELECT date, value,
  LAG(value) OVER (ORDER BY date) as prev_month,
  value - LAG(value) OVER (ORDER BY date) as change_pts
FROM public.economic_indicators 
WHERE indicator_code = 'UNRATE' AND date >= CURRENT_DATE - INTERVAL '12 months';

-- Compare inflation indicators
SELECT date,
  (SELECT value FROM public.economic_indicators 
   WHERE indicator_code = 'CPIAUCSL' AND ei.date = date) as cpi,
  (SELECT value FROM public.economic_indicators 
   WHERE indicator_code = 'CPILFESL' AND ei.date = date) as cpi_core
FROM public.economic_indicators ei
WHERE date >= CURRENT_DATE - INTERVAL '24 months';
```

---

### 6. `06_create_pipeline_metadata.sql`
**Purpose:** Audit trail and monitoring for all ETL pipeline executions  
**Records:** Metadata about each pipeline run (timestamps, counts, status, errors)  
**Key Features:**
- Unique constraint on run_id for idempotent operations
- 9 strategic indexes for operational dashboards
- Stored procedure: `mark_pipeline_run_complete()` for updating run status
- Tracks execution status, performance metrics, data quality flags
- Captures parameters, error messages, anomaly details
- Supports retry tracking and dependency management

**Example Data:**
- Stock ETL run: "stock_20260107_143025_abc123"
- Weather ETL run: "weather_20260107_150000_def456"
- Tracks: extract/transform/load phases, success/failure/warning counts

**Common Queries:**
```sql
-- Latest run for each pipeline
SELECT DISTINCT ON (pipeline_name)
    pipeline_name, run_id, started_at, status, total_duration_seconds
FROM public.pipeline_metadata
ORDER BY pipeline_name, started_at DESC;

-- Failed runs requiring investigation
SELECT run_id, pipeline_name, started_at, error_message, failure_count
FROM public.pipeline_metadata
WHERE status = 'failed' AND started_at >= CURRENT_DATE - INTERVAL '7 days';

-- Pipeline performance trends
SELECT DATE(started_at) as run_date, pipeline_name,
  COUNT(*) as runs,
  AVG(total_duration_seconds) as avg_duration_sec,
  SUM(records_loaded) as total_records_loaded
FROM public.pipeline_metadata
WHERE started_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(started_at), pipeline_name;
```

---

## Deployment Instructions

### Single File Execution
To create all tables at once:
```bash
psql -h <hostname> -U <username> -d <database> -f init.sql
```

Or create a master init.sql that includes all files:
```bash
cat 01_*.sql 02_*.sql 03_*.sql 04_*.sql 05_*.sql 06_*.sql > init.sql
psql -h <hostname> -U <username> -d <database> -f init.sql
```

### Docker Compose Integration
The `docker-compose.yml` automatically runs these scripts via the `init.sql/` directory:
```yaml
services:
  postgres:
    image: postgres:15
    volumes:
      - ./init.sql:/docker-entrypoint-initdb.d
```

---

## Schema Design Principles

### 1. **Upsert-Friendly Unique Constraints**
All tables use composite unique constraints that allow efficient upsert operations:
- forex_rates: `(from_currency, to_currency, date)`
- stock_prices: `(symbol, date)`
- weather_data: `(city, date, COALESCE(hour, -1))`
- economic_indicators: `(indicator_code, date, seasonal_adjustment, region)`

This prevents duplicate data from pipeline retries.

### 2. **Strategic Indexing**
Each table has 6-9 carefully chosen indexes:
- **Exact match indexes**: Most common lookup patterns (symbol, city, indicator_code)
- **Range indexes**: Date-based queries (DESC order for recent data first)
- **Composite indexes**: Multi-column queries (symbol+date range)
- **Temporal indexes**: Recent data access (created_at, started_at)
- **Upsert indexes**: Conflict detection optimization
- **Filtered indexes**: Rare event detection (anomalies, preliminary data)

### 3. **Data Validation Constraints**
Comprehensive CHECK constraints ensure data quality:
- OHLC logic: high >= low >= open/close
- Range checks: humidity 0-100%, wind direction 0-360°
- Sign enforcement: positive prices, non-negative volumes
- Format validation: symbol format, currency codes, status values

### 4. **Audit Trail Support**
Every table includes:
- `id` (UUID): Unique record identifier
- `created_at`: First insertion timestamp
- `updated_at`: Last modification timestamp (auto-updated via trigger)
- Triggers for automatic timestamp management

### 5. **Metadata & Lineage**
Tables include source tracking to understand data origin:
- `source`: Which API/system provided the data
- `data_release_date`: When source published the data
- `parameters`: What input parameters were used
- `environment`: Prod vs. staging vs. dev

---

## Table Relationships

```
forex_rates ──┐
stock_prices ─┼─→ pipeline_metadata (tracks execution)
weather_data ─┤
economic_indicators ┘
```

The `pipeline_metadata` table tracks the execution of all ETL pipelines, providing:
- Observability into what data was loaded when
- Root cause analysis for failed runs
- Performance trending and optimization opportunities
- Data lineage for auditing and compliance

---

## Performance Optimization Tips

### 1. **Index Maintenance**
```sql
-- Analyze table statistics (improve query planning)
ANALYZE public.stock_prices;

-- Vacuum to reclaim space and update visibility map
VACUUM ANALYZE public.stock_prices;

-- Rebuild indexes if fragmented (100M+ rows)
REINDEX INDEX idx_stock_prices_symbol_date;
```

### 2. **Partition Strategy (for very large tables)**
Consider partitioning by date for tables with millions of rows:
```sql
CREATE TABLE stock_prices_2024 PARTITION OF stock_prices
  FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

### 3. **Materialized Views (for analytics)**
Create summary views for common queries:
```sql
CREATE MATERIALIZED VIEW stock_daily_stats AS
SELECT symbol, date,
  AVG(close) as avg_close,
  MAX(high) as max_high,
  MIN(low) as min_low,
  SUM(volume) as total_volume
FROM stock_prices
GROUP BY symbol, date;
```

---

## Monitoring & Alerts

### Row Count Monitoring
```sql
-- Check table sizes
SELECT schemaname, tablename, 
  round(pg_total_relation_size(schemaname||'.'||tablename) / 1024 / 1024) as size_mb
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Failed ETL Runs
```sql
-- Alert on failed pipelines
SELECT pipeline_name, COUNT(*) as failed_runs
FROM pipeline_metadata
WHERE status = 'failed' AND started_at >= CURRENT_DATE - INTERVAL '24 hours'
GROUP BY pipeline_name;
```

---

## Related Documentation

- [Architecture Diagram](../Architecture%20Diagram.md): System overview
- [FOREX_GUIDE.md](../FOREX_GUIDE.md): Forex extraction details
- [design.md](../design.md): System design patterns
- [run_etl.py](../run_etl.py): Python ETL implementation

---

## Support & Troubleshooting

### Connection Issues
```bash
# Test PostgreSQL connection
psql -h localhost -U postgres -d financial_etl -c "SELECT 1;"
```

### Schema Validation
```sql
-- List all tables
SELECT tablename FROM pg_tables WHERE schemaname = 'public';

-- List table columns
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_name = 'stock_prices';

-- Check indexes
SELECT indexname FROM pg_indexes WHERE tablename = 'stock_prices';
```

### Data Validation
```sql
-- Check for constraint violations
SELECT * FROM stock_prices WHERE high < low OR high < open OR high < close;

-- Identify missing data
SELECT symbol, COUNT(DISTINCT date) as trading_days
FROM stock_prices
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY symbol;
```

---

**Last Updated:** 2026-01-07  
**Database Version:** PostgreSQL 15+  
**Python ETL Version:** Compatible with financial-etl-pipeline v1.0+
