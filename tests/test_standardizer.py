

# tests/test_standardizer.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.transform.standardizer import DataStandardizer

@pytest.fixture
def standardizer():
    return DataStandardizer()

def create_stock_df():
    return pd.DataFrame({
        "Open": [100, 102, 104],
        "High": [101, 103, 105],
        "Low": [99, 101, 103],
        "Close": [100.5, 102.5, 104.5],
        "Volume": [1000, 1100, 1200],
        "Ticker": ["AAPL.US", "AAPL.US", "AAPL.US"],
        "Date": pd.date_range("2026-01-01", periods=3, freq="D"),
        "Currency": ["USD", "USD", "USD"]
    })

def create_crypto_df():
    return pd.DataFrame({
        "open_price": [50000, 50500, 51000],
        "high_price": [50500, 51000, 51500],
        "low_price": [49500, 50000, 50500],
        "close_price": [50200, 50700, 51200],
        "vol": [10, 12, 15],
        "symbol": ["BTC-USD", "BTC-USD", "BTC-USD"],
        "timestamp": pd.date_range("2026-01-01", periods=3, freq="H"),
        "currency": ["USD", "USD", "USD"]
    })

def create_forex_df():
    return pd.DataFrame({
        "Open": [1.10, 1.11, 1.12],
        "High": [1.11, 1.12, 1.13],
        "Low": [1.09, 1.10, 1.11],
        "Close": [1.105, 1.115, 1.125],
        "Pair": ["EUR/USD", "EUR/USD", "EUR/USD"],
        "Time": pd.date_range("2026-01-01", periods=3, freq="H")
    })

def create_economic_df():
    return pd.DataFrame({
        "series_code": ["GDP", "GDP", "GDP"],
        "value": [1000, 1010, 1020],
        "observation_date": pd.date_range("2026-01-01", periods=3, freq="D"),
        "units": ["US Dollars", "US Dollars", "US Dollars"]
    })

def test_stock_standardization(standardizer):
    df = create_stock_df()
    df_std = standardizer.standardize_dataframe(df, data_type="stock")
    
    # Check columns
    expected_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'standardized_at', 'data_type']
    for col in expected_cols:
        assert col in df_std.columns
    
    # Check symbol standardization
    assert all(df_std['symbol'] == 'AAPL')
    
    # Check timestamp conversion
    assert pd.api.types.is_datetime64_any_dtype(df_std['timestamp'])

def test_crypto_standardization(standardizer):
    df = create_crypto_df()
    df_std = standardizer.standardize_dataframe(df, data_type="crypto")
    
    # Symbol conversion
    assert all(df_std['symbol'] == 'BTCUSDT')
    
    # Price columns
    for col in ['open', 'high', 'low', 'close']:
        assert pd.api.types.is_float_dtype(df_std[col])
    
    # Granularity column
    assert 'granularity' in df_std.columns

def test_forex_standardization(standardizer):
    df = create_forex_df()
    df_std = standardizer.standardize_dataframe(df, data_type="forex")
    
    # Symbol conversion
    assert all(df_std['symbol'] == 'EURUSD')
    
    # Timestamp
    assert pd.api.types.is_datetime64_any_dtype(df_std['timestamp'])

def test_economic_standardization(standardizer):
    df = create_economic_df()
    df_std = standardizer.standardize_dataframe(df, data_type="economic")
    
    # Columns
    for col in ['timestamp', 'series_id', 'value', 'units']:
        assert col in df_std.columns
    
    # Units standardized
    assert all(df_std['units'] == 'usd')
    
    # Value type
    assert pd.api.types.is_float_dtype(df_std['value'])
