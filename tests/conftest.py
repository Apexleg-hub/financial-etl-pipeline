

# tests/conftest.py
import pytest
import sys
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))


@pytest.fixture
def mock_twelve_data_config():
    """Mock Twelve Data configuration"""
    return {
        'twelve_data': {
            'base_url': 'https://api.twelvedata.com',
            'api_key': 'test_api_key_123',
            'default_interval': '1day',
            'max_data_points': 5000,
            'forex': {'default_pairs': ['EUR/USD', 'GBP/USD']},
            'stocks': {'default_stocks': ['AAPL', 'MSFT']},
            'crypto': {'default_cryptos': ['BTC/USD', 'ETH/USD']},
            'etfs': {'default_etfs': ['SPY', 'QQQ']},
            'indices': {'default_indices': ['DJI', 'GSPC']}
        }
    }


@pytest.fixture
def sample_time_series_data():
    """Sample time series data for testing"""
    return {
        "meta": {
            "symbol": "TEST",
            "interval": "1day",
            "currency": "USD"
        },
        "values": [
            {"datetime": "2024-01-01", "open": "100.0", "high": "102.0", "low": "98.0", "close": "101.0", "volume": "1000000"},
            {"datetime": "2024-01-02", "open": "101.0", "high": "103.0", "low": "99.0", "close": "102.0", "volume": "1100000"}
        ]
    }