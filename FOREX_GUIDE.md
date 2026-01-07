# Forex Data Extraction Guide

## Overview

The financial ETL pipeline includes complete support for extracting forex (foreign exchange) data using the Alpha Vantage API. The implementation follows the same pattern as other data sources (stock, weather, etc.).

## Quick Start

### 1. Extract Forex Data Only


python run_etl.py --pipelines forex


### 2. Extract Specific Currency Pairs

```bash
# EUR/USD and GBP/USD only
python run_etl.py --pipelines forex
```

Then modify `config/pipeline_config.yaml`:
```yaml
sources:
  forex:
    enabled: true
    api_key: ${ALPHA_VANTAGE_API_KEY}
    pairs:
      - [EUR, USD]
      - [GBP, USD]
    timeframe: daily
```

### 3. Run with All Pipelines

```bash
python run_etl.py --pipelines all
```

This will run: stock, weather, fred, finnhub, and **forex** ETL pipelines.

## Configuration

### Available Currency Pairs

**Major Pairs (highest liquidity):**
```yaml
pairs:
  - [EUR, USD]  # Euro/US Dollar
  - [USD, JPY]  # US Dollar/Japanese Yen
  - [GBP, USD]  # British Pound/US Dollar
  - [USD, CHF]  # US Dollar/Swiss Franc
  - [AUD, USD]  # Australian Dollar/US Dollar
  - [USD, CAD]  # US Dollar/Canadian Dollar
  - [NZD, USD]  # New Zealand Dollar/US Dollar
  - [USD, CNY]  # US Dollar/Chinese Yuan
```

**Minor Pairs:**
```yaml
pairs:
  - [EUR, GBP]  # Euro/British Pound
  - [EUR, JPY]  # Euro/Japanese Yen
  - [GBP, JPY]  # British Pound/Japanese Yen
  - [EUR, CHF]  # Euro/Swiss Franc
  - [AUD, JPY]  # Australian Dollar/Japanese Yen
```

### Configuration File

**`config/pipeline_config.yaml`:**
```yaml
sources:
  forex:
    enabled: true
    api_key: ${ALPHA_VANTAGE_API_KEY}  # Uses ALPHA_VANTAGE_API_KEY from .env
    pairs:
      - [EUR, USD]
      - [GBP, USD]
      - [USD, JPY]
    timeframe: daily  # daily, weekly, or monthly
```

### Data Schema

Forex data is stored with the following fields:

| Field | Type | Description |
|-------|------|-------------|
| from_currency | str | Base currency code (e.g., "EUR") |
| to_currency | str | Quote currency code (e.g., "USD") |
| date | datetime | Date of the exchange rate |
| open | float | Opening exchange rate |
| high | float | Highest exchange rate for the day |
| low | float | Lowest exchange rate for the day |
| close | float | Closing exchange rate |

**Example Record:**
```
from_currency: EUR
to_currency: USD
date: 2026-01-07
open: 1.0852
high: 1.0923
low: 1.0821
close: 1.0891
```

## Python Usage

### Extract Forex Data Programmatically

```python
from src.extract.forex_extractor import ForexExtractor
from src.transform.data_cleaner import DataCleaner
from src.load.supabase_loader import SupabaseLoader
from src.load.data_models import ForexRate
from config.settings import settings

# Initialize extractor
extractor = ForexExtractor(api_key=settings.alpha_vantage_api_key)

# Extract daily rates for EUR/USD
df = extractor.extract_forex_data(
    from_symbol="EUR",
    to_symbol="USD",
    timeframe="daily",
    output_size="compact"
)

print(f"Extracted {len(df)} records for EUR/USD")
print(df.head())
```

### Full ETL Pipeline Example

```python
from run_etl import run_forex_etl

# Run with default pairs from config
success = run_forex_etl()

# Or run with specific pairs
pairs = [["EUR", "USD"], ["GBP", "USD"], ["USD", "JPY"]]
success = run_forex_etl(pairs=pairs)

print(f"Forex ETL {'succeeded' if success else 'failed'}")
```

## Data Flow

```
1. EXTRACT
   └─> ForexExtractor.extract_forex_data(from_currency, to_currency)
       └─> Alpha Vantage API FX_DAILY endpoint
       └─> Returns DataFrame with OHLC data

2. TRANSFORM
   ├─> DataCleaner (removes duplicates, handles nulls)
   ├─> DataStandardizer (normalizes column names, formats)
   └─> DataValidator (checks data quality, anomalies)

3. LOAD
   └─> SupabaseLoader.load_from_dataframe()
       └─> Converts Timestamp objects to ISO format strings
       └─> Upserts into 'forex_rates' table
       └─> Creates conflict constraints on [from_currency, to_currency, date]
```

## API Rate Limits

Alpha Vantage has the following rate limits for the free tier:

- **5 requests per minute** (standard)
- **500 requests per day** (standard)
- Premium tier: 360/minute, 120K/day

The pipeline automatically includes:
- Rate limiter that enforces 5 req/min
- Exponential backoff retry logic (up to 5 attempts)
- Smart detection of rate-limit responses

### Built-in Rate Limiting

The code includes automatic rate limiting:

```python
# Extracts from multiple pairs with automatic delays
pairs = [["EUR", "USD"], ["GBP", "USD"], ["USD", "JPY"]]
for pair in pairs:
    df = extractor.extract_forex_data(pair[0], pair[1])  # Auto-delayed!
```

## Key Features

### ✅ Automatic Retry on Rate Limits

When Alpha Vantage returns a rate limit response, the system automatically retries with exponential backoff:

- Attempt 1: Wait 4 seconds
- Attempt 2: Wait 8 seconds  
- Attempt 3: Wait 16 seconds
- Attempt 4: Wait 32 seconds
- Attempt 5: Wait 64 seconds (max)

### ✅ Timestamp Serialization

All timestamp fields are automatically converted to ISO format strings for JSON compatibility:

```python
# Before: Timestamp('2026-01-07 12:00:00')
# After:  '2026-01-07T12:00:00+00:00'
```

### ✅ Anomaly Detection (Configurable)

The pipeline detects unusual exchange rates via Z-score analysis:

```yaml
transformation:
  allow_anomalies: true           # Continue despite anomalies
  anomalies_as_warnings: true     # Log as warnings, not errors
  anomaly_zscore_threshold: 3.0   # Z-score threshold
```

### ✅ Flexible Configuration

Change behavior without modifying code:

```yaml
# Use config file
transformation:
  allow_anomalies: true

# Or override at runtime
from src.transform.data_cleaner import DataCleaner
cleaner = DataCleaner()
# cleaner.allow_anomalies is auto-loaded from config
```

## Troubleshooting

### "API limit reached"

This is normal with the free tier. The system will automatically retry. To extract more pairs:

1. Use premium API key (360 req/min)
2. Reduce number of pairs
3. Extract weekly/monthly instead of daily

### "Unexpected response format"

Possible causes:
- Invalid currency pair code
- API key is missing/invalid
- API changed response format

**Solution:**
```python
# Check if pair is valid
from src.extract.forex_extractor import get_major_currency_pairs, get_minor_currency_pairs

major = get_major_currency_pairs()
minor = get_minor_currency_pairs()
```

### "ModuleNotFoundError: No module named 'config'"

This has been fixed in the logger. If you still see it, ensure you're running from the project root:

```bash
cd /path/to/financial-etl-pipeline
python run_etl.py --pipelines forex
```

## Testing

Run the forex extractor tests:

```bash
pytest tests/test_forex_extractor.py -v
```

Example test:
```python
def test_extract_forex_data():
    """Test forex data extraction"""
    extractor = ForexExtractor(api_key="demo")
    
    df = extractor.extract_forex_data("EUR", "USD", timeframe="daily")
    
    assert isinstance(df, pd.DataFrame)
    assert "open" in df.columns
    assert "close" in df.columns
```

## Data Storage

Forex data is stored in the **`forex_rates`** table with:
- **Primary Key**: [from_currency, to_currency, date]
- **Upsert Strategy**: Update if exists, insert if new
- **Indexing**: Recommended on [from_currency, to_currency, date]

### Sample Query (Supabase/PostgreSQL)

```sql
-- Get latest EUR/USD rates
SELECT * FROM forex_rates
WHERE from_currency = 'EUR' AND to_currency = 'USD'
ORDER BY date DESC
LIMIT 30;

-- Get all currency pairs on a specific date
SELECT DISTINCT from_currency, to_currency
FROM forex_rates
WHERE date = '2026-01-07'
ORDER BY from_currency;
```

## Advanced Usage

### Extract Multiple Timeframes

Currently supports `daily`, `weekly`, `monthly` in the `timeframe` parameter:

```python
# In config/pipeline_config.yaml
sources:
  forex:
    timeframe: weekly  # or daily, monthly
```

### Custom Extractor Configuration

```python
extractor = ForexExtractor(api_key="your-key")

# Extract 5+ years of data (full output)
df = extractor.extract_forex_data(
    from_symbol="EUR",
    to_symbol="USD",
    timeframe="daily",
    output_size="full"  # vs "compact" (100 days)
)
```

## Monitoring

Check logs for forex pipeline execution:

```bash
# View recent logs
tail -f logs/etl_*.log

# Filter for forex
grep "Forex ETL" logs/etl_*.log
```

Logs will show:
```json
{
  "timestamp": "2026-01-07T12:00:00+00:00",
  "level": "INFO",
  "message": "Starting Forex ETL for pairs: [['EUR', 'USD']]"
}
```

## Next Steps

1. **Set ALPHA_VANTAGE_API_KEY** in `.env`
2. **Configure pairs** in `config/pipeline_config.yaml`
3. **Run the pipeline**: `python run_etl.py --pipelines forex`
4. **Monitor logs** for success/errors
5. **Query data** from `forex_rates` table in Supabase

## Support

For issues:
- Check API key is valid: `echo $ALPHA_VANTAGE_API_KEY`
- Verify currency pair codes are uppercase
- Check rate limit hasn't been exceeded
- Review logs for detailed error messages
