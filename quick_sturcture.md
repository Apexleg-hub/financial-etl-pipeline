.
├── .github
│   └── workflows
│       └── ci.yml
├── .pytest_cache
│   ├── v
│   │   └── cache
│   │       ├── lastfailed
│   │       └── nodeids
│   ├── .gitignore
│   ├── CACHEDIR.TAG
│   └── README.md
├── airflow
│   └── logs
├── config
│   ├── __pycache__
│   │   ├── __init__.cpython-310.pyc
│   │   ├── __init__.cpython-312.pyc
│   │   ├── settings.cpython-310.pyc
│   │   └── settings.cpython-312.pyc
│   ├── sources
│   │   └── twelve_data.yaml
│   ├── __init__.py
│   ├── database.yaml
│   ├── pipeline_config.yaml
│   ├── settings.py
│   ├── sources.yaml
│   └── twelve_data_config.py
├── dags
│   ├── alpha_vantage_etl.py
│   └── financial_apis.py
├── data
├── examples
│   └── twelve_data_example.py
├── financial_etl_pipeline.egg-info
│   ├── dependency_links.txt
│   ├── PKG-INFO
│   ├── SOURCES.txt
│   └── top_level.txt
├── init.sql
│   ├── 00_master_init.sql
│   ├── 01_create_forex_rates.sql
│   ├── 02_forex_rates_operations.sql
│   ├── 03_create_stock_prices.sql
│   ├── 04_create_weather_data.sql
│   ├── 05_create_economic_indicators.sql
│   ├── 06_create_pipeline_metadata.sql
│   ├── 07_create_forex_mt5.sql
│   ├── 08_fix_pipeline_metadata.sql
│   └── README.md
├── k8s
│   └── cicd.yaml
├── logs
│   ├── scheduler
│   │   ├── 2025-12-27
│   │   ├── 2025-12-28
│   │   ├── 2025-12-29
│   │   ├── 2025-12-30
│   │   ├── 2026-01-05
│   │   ├── 2026-01-06
│   │   ├── 2026-01-07
│   │   └── 2026-01-08
│   ├── etl_pipeline_20260105.log
│   ├── etl_pipeline_20260106.log
│   ├── etl_pipeline_20260107.log
│   ├── etl_pipeline_20260108.log
│   ├── etl_pipeline_20260110.log
│   ├── etl_pipeline_20260111.log
│   └── scheduler.log
├── plugins
├── scripts
│   ├── quick_test.py
│   ├── setup-cicd.sh
│   └── test_twelve_data_end_to_end.py
├── src
│   ├── __pycache__
│   │   ├── __init__.cpython-310.pyc
│   │   ├── __init__.cpython-312.pyc
│   │   └── cli.cpython-310.pyc
│   ├── extract
│   │   ├── __pycache__
│   │   │   ├── __init__.cpython-312.pyc
│   │   │   ├── alpha_vantage.cpython-312.pyc
│   │   │   ├── base_extractor.cpython-312.pyc
│   │   │   ├── crypto.cpython-312.pyc
│   │   │   ├── finnhub.cpython-312.pyc
│   │   │   ├── forex_extractor.cpython-312.pyc
│   │   │   ├── fred.cpython-312.pyc
│   │   │   └── weather.cpython-312.pyc
│   │   ├── twelve_data
│   │   │   ├── __pycache__
│   │   │   │   ├── __init__.cpython-312.pyc
│   │   │   │   ├── base.cpython-312.pyc
│   │   │   │   ├── etfs_indices.cpython-312.pyc
│   │   │   │   ├── factory.cpython-312.pyc
│   │   │   │   ├── forex.cpython-312.pyc
│   │   │   │   └── time_series.cpython-312.pyc
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   ├── crypto.py
│   │   │   ├── etfs_indices.py
│   │   │   ├── etfs.py
│   │   │   ├── factory.py
│   │   │   ├── forex.py
│   │   │   ├── stocks.py
│   │   │   └── time_series.py
│   │   ├── __init__.py
│   │   ├── base_extractor.py
│   │   ├── finnhub.py
│   │   ├── fred.py
│   │   ├── mt5.py
│   │   ├── polygon_extractor.py
│   │   ├── weather_utils.py
│   │   ├── weather.py
│   │   └── yfinance.py
│   ├── load
│   │   ├── __pycache__
│   │   │   ├── __init__.cpython-312.pyc
│   │   │   ├── data_models.cpython-312.pyc
│   │   │   └── supabase_loader.cpython-312.pyc
│   │   ├── __init__.py
│   │   ├── data_models.py
│   │   └── supabase_loader.py
│   ├── monitoring
│   │   ├── __pycache__
│   │   │   ├── __init__.cpython-312.pyc
│   │   │   └── alerting.cpython-312.pyc
│   │   ├── __init__.py
│   │   └── alerting.py
│   ├── orchestration
│   │   ├── dags
│   │   │   └── etl_pipeline.py
│   │   └── __init__.py
│   ├── pipelines
│   │   └── forex_pipeline.py
│   ├── transform
│   │   ├── __pycache__
│   │   │   ├── __init__.cpython-312.pyc
│   │   │   ├── data_cleaner.cpython-312.pyc
│   │   │   ├── feature_engineer.cpython-312.pyc
│   │   │   ├── standardizer.cpython-312.pyc
│   │   │   └── validator.cpython-312.pyc
│   │   ├── __init__.py
│   │   ├── data_cleaner.py
│   │   ├── feature_engineer.py
│   │   ├── standardizer.py
│   │   └── validator.py
│   ├── utils
│   │   ├── __pycache__
│   │   │   ├── __init__.cpython-310.pyc
│   │   │   ├── __init__.cpython-312.pyc
│   │   │   ├── logger.cpython-310.pyc
│   │   │   ├── logger.cpython-312.pyc
│   │   │   └── rate_limiter.cpython-312.pyc
│   │   ├── __init__.py
│   │   ├── logger.py
│   │   └── rate_limiter.py
│   ├── __init__.py
│   └── cli.py
├── tests
│   ├── __pycache__
│   │   ├── __init__.cpython-312.pyc
│   │   ├── conftest.cpython-312-pytest-9.0.2.pyc
│   │   ├── test_alpha_vantage_final.cpython-312.pyc
│   │   ├── test_crypto.cpython-312-pytest-9.0.2.pyc
│   │   ├── test_extract.cpython-312-pytest-9.0.2.pyc
│   │   ├── test_finnhub.cpython-312-pytest-9.0.2.pyc
│   │   ├── test_fix.cpython-312.pyc
│   │   ├── test_forex_extractor.cpython-312-pytest-9.0.2.pyc
│   │   ├── test_fred.cpython-312-pytest-9.0.2.pyc
│   │   ├── test_fx_extract.cpython-312.pyc
│   │   ├── test_load.cpython-312-pytest-9.0.2.pyc
│   │   ├── test_monitoring.cpython-312-pytest-9.0.2.pyc
│   │   ├── test_polygon.cpython-312.pyc
│   │   ├── test_transform.cpython-312-pytest-9.0.2.pyc
│   │   └── test_weather.cpython-312-pytest-9.0.2.pyc
│   ├── integration
│   │   ├── __init__.py
│   │   └── test_twelve_data_integration.py
│   ├── mock
│   │   ├── __init__.py
│   │   └── test_twelve_data_mock.py
│   ├── unit
│   │   ├── __init__.py
│   │   └── test_twelve_data_base.py
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_crypto.py
│   ├── test_extract.py
│   ├── test_finnhub.py
│   ├── test_forex_extractor.py
│   ├── test_fred.py
│   ├── test_fx_extract.py
│   ├── test_load.py
│   ├── test_monitoring.py
│   ├── test_mt5_extractor.py
│   ├── test_polygon.py
│   ├── test_populate.py
│   ├── test_standardizer.py
│   ├── test_transform.py
│   └── test_weather.py
├── .dockerignore
├── .env
├── .env.ci
├── .env.example
├── .env.template
├── .gitignore
├── .gitlab-ci.yml
├── .pre-commit-config.yaml
├── Architecture Diagram.md
├── CI_CD_REVIEW.md
├── COVERAGE_ANALYSIS.md
├── COVERAGE_QUICK_REFERENCE.md
├── COVERAGE_REPORT.md
├── DataSource.md
├── debug_api_response.py
├── design.md
├── docker-compose.test.yml
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.ci
├── ETL documentation.md
├── FOREX_GUIDE.md
├── Jenkinsfile
├── Makefile
├── MT5_INTEGRATION_GUIDE.md
├── project_sturcture.md
├── pyproject.toml
├── pytest.ini
├── README.md
├── requirements-dev.txt
├── requirements-pinned.txt
├── requirements.txt
├── run_etl.py
├── SCHEDULER_GUIDE.md
├── scheduler.py
├── setup_test_env.sh
├── setup_windows_scheduler.py
└── setup.py