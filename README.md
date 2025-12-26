# Financial Data ETL Pipeline

A production-ready ETL (Extract, Transform, Load) pipeline for collecting, processing, and storing financial market data from multiple sources including stock markets, cryptocurrencies, and economic indicators.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Data Sources                            │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
│  │ Alpha   │  │Finnhub  │  │  FRED   │  │ Crypto  │       │
│  │Vantage  │  │   API   │  │   API   │  │   API   │       │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘       │
└───────┼───────────┼───────────┼───────────┼────────────────┘
        │           │           │           │
        └───────────┴───────────┴───────────┘
                    │
        ┌───────────▼──────────────┐
        │   Extract Layer          │
        │  - API Clients           │
        │  - Rate Limiting         │
        │  - Error Handling        │
        └───────────┬──────────────┘
                    │
        ┌───────────▼──────────────┐
        │   Transform Layer        │
        │  - Data Cleaning         │
        │  - Validation            │
        │  - Feature Engineering   │
        │  - Standardization       │
        └───────────┬──────────────┘
                    │
        ┌───────────▼──────────────┐
        │   Load Layer             │
        │  - Supabase Loader       │
        │  - Batch Processing      │
        │  - Upsert Logic          │
        └───────────┬──────────────┘
                    │
        ┌───────────▼──────────────┐
        │   Supabase Database      │
        │  - Stock Prices          │
        │  - Crypto Prices         │
        │  - Economic Indicators   │
        │  - ETL Metadata          │
        └──────────────────────────┘
```

## 🚀 Features

- **Multi-Source Data Collection**: Extracts data from:
  - Alpha Vantage (Stock prices, company fundamentals)
  - Finnhub (Real-time market data)
  - FRED (Economic indicators)
  - Cryptocurrency APIs (Bitcoin, Ethereum, etc.)
  - Weather data (for correlational analysis)

- **Robust Data Processing**:
  - Automatic data cleaning and validation
  - Duplicate detection and removal
  - Type conversion and standardization
  - Missing value handling
  - Data quality checks

- **Production-Ready Features**:
  - Airflow orchestration for scheduling
  - Rate limiting and retry logic
  - Comprehensive logging
  - Error handling and alerting
  - Monitoring with Prometheus & Grafana
  - Docker containerization

- **Data Storage**:
  - Supabase (PostgreSQL) for persistent storage
  - Batch loading with upsert capability
  - Metadata tracking for pipeline runs

## 📋 Prerequisites

- Python 3.9+
- Docker & Docker Compose
- API Keys for:
  - Alpha Vantage
  - Finnhub
  - FRED
- Supabase account

## 🛠️ Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd financial-etl-pipeline
```

### 2. Create virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your API keys:

```env
# Alpha Vantage
ALPHA_VANTAGE_API_KEY=your_key_here

# Finnhub
FINNHUB_API_KEY=your_key_here

# FRED
FRED_API_KEY=your_key_here

# Supabase
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key

# Airflow
AIRFLOW_FERNET_KEY=generate_with_python_cryptography
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin

# Pipeline Configuration
STOCK_SYMBOLS=AAPL,GOOGL,MSFT,AMZN,TSLA
CRYPTO_SYMBOLS=BTC,ETH,ADA,SOL
FRED_SERIES=GDP,UNRATE,CPIAUCSL
```

### 5. Generate Airflow Fernet Key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## 🐳 Docker Deployment

### Start all services

```bash
cd docker
docker-compose up -d
```


This will start:
- PostgreSQL (Airflow metadata)
- Redis (Celery broker)
- Airflow Webserver (port 8080)
- Airflow Scheduler
- Airflow Worker
- Flower (Celery monitoring, port 5555)
- Prometheus (port 9090)
- Grafana (port 3000)

### Access services

- Airflow UI: http://localhost:8080
- Flower: http://localhost:5555
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090

### Stop services

```bash
docker-compose down
```

### View logs

```bash
docker-compose logs -f airflow-scheduler
```

## 💻 Usage

### Running the ETL Pipeline

#### Using Airflow (Recommended for Production)

1. Access Airflow UI at http://localhost:8080
2. Enable the `financial_data_etl_pipeline` DAG
3. Trigger manually or wait for scheduled run (6 PM weekdays)

#### Running Standalone Scripts

Extract stock data:
```bash
python -m src.extract.alpha_vantage
```

Run full pipeline:
```bash
python scripts/run_etl.py
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_extract.py
```

## 📁 Project Structure

```
financial-etl-pipeline/
├── config/                      # Configuration files
│   ├── database.yaml           # Database configuration
│   ├── pipeline_config.yaml    # Pipeline settings
│   ├── settings.py             # Application settings
│   └── sources.yaml            # Data source configs
├── docker/                      # Docker configuration
│   ├── docker-compose.yml      # Service orchestration
│   └── Dockerfile              # Container definition
├── src/                         # Source code
│   ├── extract/                # Data extraction
│   │   ├── base_extractor.py  # Base class for extractors
│   │   ├── alpha_vantage.py   # Stock data extractor
│   │   ├── finnhub.py          # Market data extractor
│   │   ├── fred.py             # Economic data extractor
│   │   ├── crypto.py           # Crypto data extractor
│   │   └── weather.py          # Weather data extractor
│   ├── transform/              # Data transformation
│   │   ├── data_cleaner.py    # Data cleaning logic
│   │   ├── feature_engineer.py # Feature creation
│   │   ├── standardizer.py    # Data standardization
│   │   └── validator.py        # Data validation
│   ├── load/                   # Data loading
│   │   ├── data_models.py     # Pydantic models
│   │   └── supabase_loader.py # Database loader
│   ├── orchestration/          # Pipeline orchestration
│   │   └── dags/
│   │       └── etl_pipeline.py # Airflow DAG
│   ├── monitoring/             # Monitoring & alerts
│   │   └── alerting.py
│   └── utils/                  # Utility functions
│       ├── logger.py           # Logging configuration
│       └── rate_limiter.py     # API rate limiting
├── tests/                      # Test suite
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── .env.example                # Example environment file
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## 🔧 Configuration

### Pipeline Configuration (`config/pipeline_config.yaml`)

```yaml
extraction:
  batch_size: 100
  retry_attempts: 3
  timeout: 30

transformation:
  remove_duplicates: true
  fill_missing: true
  outlier_threshold: 3

loading:
  batch_size: 1000
  upsert_enabled: true
```

### Data Source Configuration (`config/sources.yaml`)

```yaml
alpha_vantage:
  rate_limit: 5  # requests per minute
  symbols:
    - AAPL
    - GOOGL
    - MSFT

fred:
  series:
    - GDP
    - UNRATE
    - CPIAUCSL


## 📊 Database Schema

### Stock Prices Table

```sql
CREATE TABLE stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    volume BIGINT,
    adjusted_close DECIMAL(10,2),
    data_source VARCHAR(50),
    extraction_timestamp TIMESTAMP,
    UNIQUE(symbol, date)
);
```

### Crypto Prices Table

```sql
CREATE TABLE crypto_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    price_usd DECIMAL(15,2),
    volume_24h DECIMAL(20,2),
    market_cap DECIMAL(20,2),
    change_24h DECIMAL(5,2),
    data_source VARCHAR(50),
    extraction_timestamp TIMESTAMP,
    UNIQUE(symbol, timestamp)
);
```

### Economic Indicators Table

```sql
CREATE TABLE economic_indicators (
    id SERIAL PRIMARY KEY,
    series_id VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    value DECIMAL(15,4),
    series_name VARCHAR(200),
    units VARCHAR(50),
    frequency VARCHAR(20),
    data_source VARCHAR(50),
    extraction_timestamp TIMESTAMP,
    UNIQUE(series_id, date)
);
```

## 📈 Monitoring

### Metrics Tracked

- Pipeline execution time
- Records extracted/transformed/loaded
- Error rates by component
- API response times
- Database load times

### Alerting

Configure alerts in `src/monitoring/alerting.py`:
- Pipeline failures
- Data quality issues
- API rate limit warnings
- Unusual data patterns

## 🔐 Security Best Practices

1. **Never commit API keys** - Use `.env` file
2. **Rotate credentials** regularly
3. **Use read-only database users** where possible
4. **Enable SSL/TLS** for database connections
5. **Implement rate limiting** to avoid API bans
6. **Monitor access logs** for unusual activity

## 🐛 Troubleshooting

### Common Issues

**Issue**: Airflow DAG not appearing
```bash
# Check DAG syntax
python src/orchestration/dags/etl_pipeline.py

# Check Airflow logs
docker-compose logs airflow-scheduler
```

**Issue**: API rate limit exceeded
```bash
# Adjust rate limits in config/sources.yaml
# Or increase delay between requests
```

**Issue**: Database connection failed
```bash
# Verify Supabase credentials
# Check network connectivity
# Ensure database exists
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📧 Contact

For questions or support, please open an issue on GitHub.

## 🙏 Acknowledgments

- Alpha Vantage for stock market data
- FRED for economic indicators
- Finnhub for real-time market data
- Apache Airflow for orchestration
- Supabase for database hosting