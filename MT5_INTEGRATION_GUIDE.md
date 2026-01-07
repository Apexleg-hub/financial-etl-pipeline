# MT5 Forex Data Integration Guide

## Overview

MetaTrader 5 (MT5) is a professional trading platform that provides:
- **Tick Volume**: Available on all brokers
- **Real Volume**: Available on institutional brokers
- **Multiple Timeframes**: M1 (1-min) to MN1 (monthly)
- **Precise Timestamps**: Down to seconds
- **Multiple Brokers**: IC Markets, Pepperstone, XM, etc.

This guide shows how to integrate MT5 forex data with the existing Alpha Vantage pipeline.

## Key Differences: Alpha Vantage vs MT5

| Feature | Alpha Vantage | MT5 |
|---------|---|---|
| **Volume Data** |  Not provided |  Tick volume always |
| **Granularity** | Daily only | M1 to Monthly |
| **Timeframe** | DATE | TIMESTAMPTZ |
| **Real-time** | Delayed | Live tick data |
| **API Rate Limits** | 5 req/min (free) | None (local) |
| **Cost** | Free tier available | Broker dependent |
| **Table** | forex_rates | forex_mt5 |

## Installation

### 1. Install MetaTrader5 Python Module


# Install via pip
pip install MetaTrader5

# Or add to requirements.txt
echo "MetaTrader5>=5.0.45" >> requirements-dev.txt
pip install -r requirements-dev.txt


### 2. Setup MT5 Terminal

1. Download and install MetaTrader 5 from your broker
2. Launch MT5 application
3. Login with your account (Real, Demo, or Backtest)
4. Ensure the terminal is running (stays in background)
5. Navigate to View → Market Watch and add desired pairs

### 3. Configure Your Extractor

The MT5Extractor is ready to use in `src/extract/mt5.py`:


from src.extract.mt5 import MT5Extractor, MT5Config

# Basic usage (auto-connect to running MT5)
extractor = MT5Extractor(
    pairs=[('EUR', 'USD'), ('GBP', 'USD')],
    timeframe='H1',  # Hourly candles
    broker='IC Markets'
)

# With specific connection config
config = MT5Config(
    login=1234567,  # Your MT5 account number
    password="your_password",
    server="ICMarkets-Demo",
    path="C:\\Program Files\\MetaTrader 5"
)

extractor = MT5Extractor(
    pairs=[('EUR', 'USD')],
    timeframe='M15',
    config=config
)


## Usage Examples

### Extract Historical Data


from src.extract.mt5 import MT5Extractor
import pandas as pd

# Create extractor
extractor = MT5Extractor(
    pairs=[('EUR', 'USD'), ('GBP', 'USD'), ('USD', 'JPY')],
    timeframe='D1',  # Daily candles
    broker='IC Markets'
)

# Get 90 days of history
data = extractor.extract_historical('EURUSD', days_back=90)

# Convert to DataFrame for analysis
df = pd.DataFrame(data)
print(df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].head())

#                     timestamp     open     high      low    close   volume
# 0 2025-10-09 22:00:00+00:00  1.08520  1.08620  1.08480  1.08590   45320
# 1 2025-10-10 22:00:00+00:00  1.08590  1.08700  1.08560  1.08650   38950
# ...


### Extract Multiple Timeframes


extractor = MT5Extractor(
    pairs=[('EUR', 'USD')],
    timeframe='M5'  # Can be changed per call
)

# Extract different timeframes
m5_data = extractor.extract_historical('EURUSD', days_back=1)  # Last 1 day of M5
h1_data = extractor.extract_historical('EURUSD', days_back=7)  # Last 7 days of H1

# Or create separate extractors
for timeframe in ['M1', 'M5', 'M15', 'H1', 'D1']:
    ext = MT5Extractor(
        pairs=[('EUR', 'USD')],
        timeframe=timeframe
    )
    data = ext.extract_historical('EURUSD', days_back=30)
    print(f"{timeframe}: {len(data)} candles")


### Get Symbol Information


extractor = MT5Extractor(pairs=[('EUR', 'USD')])

# Get detailed symbol info
info = extractor.get_symbol_info('EURUSD')

print(f"Symbol: {info['symbol']}")
print(f"Bid: {info['bid']}, Ask: {info['ask']}")
print(f"Spread: {info['spread']} points")
print(f"Min Volume: {info['min_volume']}, Max Volume: {info['max_volume']}")

# Output:
# Symbol: EURUSD
# Bid: 1.08521, Ask: 1.08531
# Spread: 10 points
# Min Volume: 0.01, Max Volume: 100.0


### Real-time Streaming (Live Ticks)


extractor = MT5Extractor(
    pairs=[('EUR', 'USD'), ('GBP', 'USD')]
)

# Stream real-time tick data
count = 0
for tick in extractor.extract_realtime(max_iterations=1000):
    print(f"{tick['symbol']}: Bid={tick['bid']:.5f} Ask={tick['ask']:.5f} "
          f"Volume={tick['volume']}")
    
    count += 1
    if count >= 10:
        break

# Output:
# EURUSD: Bid=1.08521 Ask=1.08531 Volume=1250000
# GBPUSD: Bid=1.26480 Ask=1.26490 Volume=950000
# EURUSD: Bid=1.08522 Ask=1.08532 Volume=1350000
# ...


## Integration with ETL Pipeline

### 1. Add MT5 Extractor to run_etl.py


from src.extract.mt5 import MT5Extractor
from src.load.data_models import ForexRateMT5

def run_mt5_etl(
    pairs: Optional[List[List[str]]] = None,
    timeframe: Optional[str] = None
) -> bool:
    """Extract and load MT5 forex data with volume"""
    try:
        if pairs is None:
            config = load_pipeline_config()
            pairs_config = config.get("sources", {}).get("mt5", {}).get("pairs", [["EUR", "USD"]])
            pairs = pairs_config
        
        if timeframe is None:
            config = load_pipeline_config()
            timeframe = config.get("sources", {}).get("mt5", {}).get("timeframe", "D1")
        
        # Extract
        extractor = MT5Extractor(pairs=pairs, timeframe=timeframe, broker="Your Broker")
        data_list = []
        
        for from_ccy, to_ccy in pairs:
            symbol = f"{from_ccy.upper()}{to_ccy.upper()}"
            data = extractor.extract_historical(symbol, days_back=30)
            data_list.extend(data)
        
        if not data_list:
            logger.warning("No MT5 data extracted")
            return False
        
        # Transform
        df = pd.DataFrame(data_list)
        cleaner = DataCleaner()
        df = cleaner.clean_dataframe(df)
        
        # Load
        loader = SupabaseLoader()
        loader.load_from_dataframe(
            df=df,
            model=ForexRateMT5,
            table_name="forex_mt5"
        )
        
        logger.info(f"Successfully loaded {len(df)} MT5 candles")
        return True
        
    except Exception as e:
        logger.error(f"MT5 ETL failed: {str(e)}")
        return False

# In main() Click group, add:
@main.command()
@click.option('--timeframe', default='D1', help='Timeframe: M1, M5, M15, H1, D1, etc')
@click.pass_context
def mt5_forex(ctx, timeframe):
    """Run MT5 forex extraction"""
    run_mt5_etl(timeframe=timeframe)


### 2. Update pipeline_config.yaml


sources:
  # ... existing sources ...
  
  mt5:
    enabled: true
    broker: "IC Markets"  # Your broker name
    pairs:
      - ["EUR", "USD"]
      - ["GBP", "USD"]
      - ["USD", "JPY"]
      - ["USD", "CHF"]
      - ["AUD", "USD"]
    timeframe: "D1"  # M1, M5, M15, M30, H1, H4, D1, W1, MN1
    days_back: 30
    account_type: "Demo"  # Real, Demo, or Backtest

transformation:
  mt5:
    allow_anomalies: true
    anomalies_as_warnings: true
    anomaly_zscore_threshold: 4.0  # Higher threshold for volume spikes


### 3. Update CLI


# Extract MT5 data with daily timeframe
python run_etl.py --pipelines mt5

# Extract MT5 data with 1-hour candles
python run_etl.py --pipelines mt5_forex --timeframe H1

# Run all pipelines including MT5
python run_etl.py --pipelines all


## Data Model

### ForexRateMT5 Table

```sql
CREATE TABLE public.forex_mt5 (
    id UUID PRIMARY KEY,
    from_currency VARCHAR(3),  -- EUR
    to_currency VARCHAR(3),    -- USD
    timestamp TIMESTAMPTZ,     -- 2026-01-07 12:34:00+00:00
    open NUMERIC(15, 6),       -- 1.085200
    high NUMERIC(15, 6),       -- 1.086200
    low NUMERIC(15, 6),        -- 1.084800
    close NUMERIC(15, 6),      -- 1.085900
    volume BIGINT,             -- 45320 (tick volume)
    real_volume BIGINT,        -- 1850000 (if available)
    timeframe VARCHAR(10),     -- H1, D1, M5, etc
    broker VARCHAR(100),       -- IC Markets
    source VARCHAR(50)         -- mt5
    -- ... plus timestamps and constraints
);

UNIQUE(from_currency, to_currency, timestamp, timeframe, broker)
```

### Python Data Model

```python
@dataclass
class ForexRateMT5(BaseModel):
    from_currency: str          # EUR
    to_currency: str            # USD
    timestamp: datetime         # 2026-01-07 12:34:00+00:00
    open: float                 # 1.085200
    high: float                 # 1.086200
    low: float                  # 1.084800
    close: float                # 1.085900
    volume: int                 # 45320
    real_volume: Optional[int]  # 1850000
    timeframe: str              # H1
    broker: Optional[str]       # IC Markets
    is_bid: bool               # True
    tick_count: Optional[int]  # Number of ticks
```

## Common Queries

### Get Latest 1-Hour Candles

```sql
SELECT timestamp, open, high, low, close, volume
FROM public.forex_mt5
WHERE from_currency = 'EUR' AND to_currency = 'USD' AND timeframe = 'H1'
  AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY timestamp DESC LIMIT 100;
```

### Volume Analysis

```sql
SELECT timeframe, 
  AVG(volume) as avg_volume,
  MAX(volume) as max_volume,
  STDDEV(volume) as volume_std
FROM public.forex_mt5
WHERE from_currency = 'EUR' AND to_currency = 'USD'
  AND timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY timeframe
ORDER BY timeframe;
```

### Compare Brokers

```sql
SELECT broker, timestamp, open, close, volume
FROM public.forex_mt5
WHERE from_currency = 'EUR' AND to_currency = 'USD' 
  AND timeframe = 'D1'
  AND timestamp = (SELECT MAX(timestamp) FROM public.forex_mt5 
                   WHERE from_currency = 'EUR' AND to_currency = 'USD')
ORDER BY broker;
```

### High-Volume Analysis (Consolidation)

```sql
SELECT timestamp, close, volume,
  AVG(volume) OVER (
    ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) as ma_20_volume
FROM public.forex_mt5
WHERE from_currency = 'EUR' AND to_currency = 'USD' AND timeframe = 'D1'
  AND volume < 0.5 * AVG(volume) OVER (
    ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  )
ORDER BY timestamp DESC;
```

## Troubleshooting

### MT5 Terminal Not Found

**Error**: `initialize() failed, error code: 1`

**Solution**:
1. Ensure MT5 application is running
2. Check MT5 installation path
3. Specify path explicitly:

```python
config = MT5Config(path="C:\\Program Files\\MetaTrader 5")
extractor = MT5Extractor(pairs=[('EUR', 'USD')], config=config)
```

### No Data Returned

**Error**: `Empty result for EURUSD`

**Solution**:
1. Verify symbol exists in your MT5 Market Watch
2. Check the symbol format (EURUSD, not EUR/USD)
3. Ensure the broker supports the pair
4. Check date range is valid

```python
# Debug symbol info
info = extractor.get_symbol_info('EURUSD')
if info:
    print("Symbol exists in MT5")
else:
    print("Symbol not found - add to Market Watch")
```

### Connection Timeout

**Error**: `initialize() timeout`

**Solution**:
1. Increase timeout:

```python
config = MT5Config(timeout=10000)  # 10 seconds
extractor = MT5Extractor(pairs=[('EUR', 'USD')], config=config)
```

2. Check firewall isn't blocking MT5
3. Restart MT5 application

### Import Error

**Error**: `No module named 'MetaTrader5'`

**Solution**:
```bash
pip install --upgrade MetaTrader5
```

## Performance Tips

1. **Local Caching**: MT5 stores data locally, so repeated queries are fast
2. **Batch Extraction**: Extract multiple pairs in one ETL run
3. **Appropriate Timeframe**: Use M5/M15 for intraday, D1 for daily analysis
4. **Index on timestamp**: Queries are fastest on TIMESTAMPTZ column
5. **Partition by Date**: For large volumes, partition forex_mt5 by date

## Security Considerations

1. **Never hardcode credentials** in code:

```python
# BAD - Never do this
config = MT5Config(login=1234567, password="password123")

# GOOD - Use environment variables
import os
config = MT5Config(
    login=int(os.getenv('MT5_LOGIN')),
    password=os.getenv('MT5_PASSWORD'),
    server=os.getenv('MT5_SERVER')
)
```

2. **Use Demo accounts** for testing
3. **Restrict database access** to forex_mt5 table
4. **Audit logs** for sensitive operations

## Next Steps

1. ✅ Install MetaTrader5 package
2. ✅ Create MT5Extractor instance
3. ✅ Test with `extract_historical()`
4. ✅ Integrate into run_etl.py
5. ✅ Configure pipeline_config.yaml
6. ✅ Run MT5 ETL pipeline
7. ✅ Monitor data in forex_mt5 table

## References

- [MetaTrader 5 Documentation](https://www.metatrader5.com/en/trading-platform)
- [Python MetaTrader5 Module](https://github.com/thinkmarkets/mt5-api)
- [MT5 Symbol Codes](https://www.investopedia.com/terms/f/forex.asp)
- [Forex Data Analysis](../Architecture%20Diagram.md)

---

**Last Updated**: 2026-01-07  
**Status**: Production Ready  
**Tested With**: MetaTrader 5 v5.0.45+, Python 3.9+
