etl_pipeline/
├── config/
│   ├── __init__.py
│   ├── settings.py
│   ├── sources.yaml
│   └── database.yaml
├── src/
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── base_extractor.py
│   │   ├── alpha_vantage.py
│   │   ├── finnhub.py
│   │   ├── fred.py
│   │   ├── crypto.py
│   │   └── weather.py
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── data_cleaner.py
│   │   ├── feature_engineer.py
│   │   ├── validator.py
│   │   └── standardizer.py
│   ├── load/
│   │   ├── __init__.py
│   │   ├── supabase_loader.py
│   │   └── data_models.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── logger.py
│   │   ├── rate_limiter.py
│   │   └── helpers.py
│   └── orchestration/
│       ├── __init__.py
│       └── dags/
│           └── etl_pipeline.py
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── .env.example
├── requirements.txt
├── pyproject.toml
└── README.md