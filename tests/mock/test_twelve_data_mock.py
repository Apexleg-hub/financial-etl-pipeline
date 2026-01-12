

# tests/mock/test_twelve_data_mock.py

#Mock Tests (For CI/CD without actual API calls)
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, AsyncMock

from src.extract.twelve_data.factory import TwelveDataExtractorFactory, AssetType
from src.extract.twelve_data.time_series import TwelveDataTimeSeriesExtractor
from src.extract.twelve_data.forex import TwelveDataForexExtractor
from src.extract.twelve_data.stocks import TwelveDataStockExtractor
from src.extract.twelve_data.crypto import TwelveDataCryptoExtractor
from src.extract.twelve_data.etfs_indices import TwelveDataETFExtractor, TwelveDataIndexExtractor


class MockTwelveDataResponse:
    """Mock responses for Twelve Data API"""
    
    @staticmethod
    def get_time_series_response():
        return {
            "meta": {
                "symbol": "TEST",
                "interval": "1day",
                "currency": "USD",
                "exchange_timezone": "America/New_York",
                "exchange": "NASDAQ",
                "mic_code": "XNAS",
                "type": "Common Stock"
            },
            "values": [
                {
                    "datetime": "2024-01-01",
                    "open": "100.00",
                    "high": "102.50",
                    "low": "99.50",
                    "close": "101.00",
                    "volume": "1000000"
                },
                {
                    "datetime": "2024-01-02",
                    "open": "101.00",
                    "high": "103.00",
                    "low": "100.50",
                    "close": "102.50",
                    "volume": "1200000"
                }
            ],
            "status": "ok"
        }
    
    @staticmethod
    def get_symbol_list_response():
        return {
            "data": [
                {
                    "symbol": "AAPL",
                    "name": "Apple Inc.",
                    "currency": "USD",
                    "exchange": "NASDAQ",
                    "mic_code": "XNAS",
                    "country": "United States",
                    "type": "Common Stock"
                },
                {
                    "symbol": "MSFT",
                    "name": "Microsoft Corporation",
                    "currency": "USD",
                    "exchange": "NASDAQ",
                    "mic_code": "XNAS",
                    "country": "United States",
                    "type": "Common Stock"
                }
            ],
            "status": "ok"
        }
    
    @staticmethod
    def get_quote_response():
        return {
            "symbol": "AAPL",
            "name": "Apple Inc.",
            "exchange": "NASDAQ",
            "mic_code": "XNAS",
            "currency": "USD",
            "datetime": "2024-01-03 16:00:00",
            "timestamp": 1704297600,
            "open": "102.50",
            "high": "103.50",
            "low": "102.00",
            "close": "103.00",
            "volume": "1500000",
            "previous_close": "101.00",
            "change": "2.00",
            "percent_change": "1.98",
            "average_volume": "1400000",
            "fifty_two_week": {
                "low": "90.00",
                "high": "150.00",
                "low_change": "13.00",
                "high_change": "-47.00",
                "low_change_percent": "14.44",
                "high_change_percent": "-31.33",
                "range": "90.00-150.00"
            }
        }


class TestTwelveDataMock:
    """Tests using mocked API responses"""
    
    def setup_method(self):
        """Setup for each test"""
        # Mock the config loading
        self.config_patcher = patch('src.extract.base_extractor.settings')
        self.mock_settings = self.config_patcher.start()
        
        # Mock config
        self.mock_config = {
            'base_url': 'https://api.twelvedata.com',
            'api_key': 'test_api_key',
            'default_interval': '1day',
            'max_data_points': 5000,
            'forex': {'default_pairs': ['EUR/USD', 'GBP/USD']},
            'stocks': {'default_stocks': ['AAPL', 'MSFT']},
            'crypto': {'default_cryptos': ['BTC/USD', 'ETH/USD']},
            'etfs': {'default_etfs': ['SPY', 'QQQ']},
            'indices': {'default_indices': ['DJI', 'GSPC']}
        }
        
        # Mock the config loading in BaseExtractor
        self.mock_settings.load_config.return_value = {'twelve_data': self.mock_config}
        self.mock_settings.get.return_value = 'test_api_key'
        self.mock_settings.timezone = 'UTC'
        self.mock_settings.max_retries = 3
    
    def teardown_method(self):
        """Teardown after each test"""
        self.config_patcher.stop()
    
    @patch('src.extract.twelve_data.base.TwelveDataExtractor._make_twelve_data_request')
    def test_time_series_extraction(self, mock_request):
        """Test time series extraction with mock"""
        mock_request.return_value = MockTwelveDataResponse.get_time_series_response()
        
        extractor = TwelveDataTimeSeriesExtractor()
        
        df = extractor.extract_time_series(
            symbol="AAPL",
            interval="1day",
            output_size=2
        )
        
        # Verify the request was made with correct parameters
        mock_request.assert_called_once()
        call_args = mock_request.call_args
        assert call_args[0][0] == "/time_series"
        assert call_args[1]['params']['symbol'] == "AAPL"
        assert call_args[1]['params']['interval'] == "1day"
        assert call_args[1]['params']['outputsize'] == 2
        
        # Verify DataFrame
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert df['symbol'].iloc[0] == "TEST"  # From mock response
        assert df['asset_type'].iloc[0] == 'stock'
        assert df['interval'].iloc[0] == '1day'
        assert 'extracted_at' in df.columns
    
    @patch('src.extract.twelve_data.base.TwelveDataExtractor._make_twelve_data_request')
    def test_forex_extraction(self, mock_request):
        """Test forex extraction with mock"""
        mock_request.return_value = MockTwelveDataResponse.get_time_series_response()
        
        extractor = TwelveDataForexExtractor()
        
        df = extractor.extract_forex_time_series(
            symbol="EUR/USD",
            interval="1day",
            output_size=2
        )
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert df['asset_type'].iloc[0] == 'forex'
        assert 'pct_change' in df.columns
    
    @patch('src.extract.twelve_data.base.TwelveDataExtractor._make_twelve_data_request')
    def test_stock_extraction(self, mock_request):
        """Test stock extraction with mock"""
        mock_request.return_value = MockTwelveDataResponse.get_time_series_response()
        
        extractor = TwelveDataStockExtractor()
        
        df = extractor.extract_stock_time_series(
            symbol="AAPL",
            interval="1day",
            output_size=2
        )
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert df['asset_type'].iloc[0] == 'stock'
        assert 'returns' in df.columns
        assert 'dollar_volume' in df.columns
    
    @patch('src.extract.twelve_data.base.TwelveDataExtractor._make_twelve_data_request')
    def test_crypto_extraction(self, mock_request):
        """Test crypto extraction with mock"""
        mock_request.return_value = MockTwelveDataResponse.get_time_series_response()
        
        extractor = TwelveDataCryptoExtractor()
        
        df = extractor.extract_crypto_time_series(
            symbol="BTC",
            quote_currency="USD",
            interval="1day",
            output_size=2
        )
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert df['asset_type'].iloc[0] == 'crypto'
        assert 'log_returns' in df.columns
        assert 'volatility_20d' in df.columns
    
    @patch('src.extract.twelve_data.base.TwelveDataExtractor._make_twelve_data_request')
    def test_etf_extraction(self, mock_request):
        """Test ETF extraction with mock"""
        mock_request.return_value = MockTwelveDataResponse.get_time_series_response()
        
        extractor = TwelveDataETFExtractor()
        
        df = extractor.extract_etf_time_series(
            symbol="SPY",
            interval="1day",
            output_size=2
        )
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert df['asset_type'].iloc[0] == 'etf'
        assert 'daily_return' in df.columns
        assert 'volatility_20d' in df.columns
    
    @patch('src.extract.twelve_data.base.TwelveDataExtractor._make_twelve_data_request')
    def test_index_extraction(self, mock_request):
        """Test index extraction with mock"""
        mock_request.return_value = MockTwelveDataResponse.get_time_series_response()
        
        extractor = TwelveDataIndexExtractor()
        
        df = extractor.extract_index_time_series(
            symbol="DJI",
            interval="1day",
            output_size=2
        )
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert df['asset_type'].iloc[0] == 'index'
        assert 'daily_return' in df.columns
        assert 'drawdown' in df.columns
    
    def test_batch_extraction(self):
        """Test batch extraction"""
        extractor = TwelveDataTimeSeriesExtractor()
        
        # Mock individual extractions
        with patch.object(extractor, 'extract_time_series') as mock_extract:
            mock_extract.side_effect = [
                pd.DataFrame({'close': [100, 101], 'symbol': ['AAPL', 'AAPL']}),
                pd.DataFrame({'close': [200, 201], 'symbol': ['MSFT', 'MSFT']}),
                pd.DataFrame({'close': [300, 301], 'symbol': ['GOOGL', 'GOOGL']})
            ]
            
            results = extractor.extract_time_series_batch(
                symbols=['AAPL', 'MSFT', 'GOOGL'],
                interval='1day',
                output_size=2,
                delay=0  # No delay for testing
            )
            
            assert len(results) == 3
            assert 'AAPL' in results
            assert 'MSFT' in results
            assert 'GOOGL' in results
            assert all(isinstance(df, pd.DataFrame) for df in results.values())
    
    def test_combine_batch_results(self):
        """Test combining batch results"""
        extractor = TwelveDataTimeSeriesExtractor()
        
        results = {
            'AAPL': pd.DataFrame({'close': [100, 101], 'date': ['2024-01-01', '2024-01-02']}),
            'MSFT': pd.DataFrame({'close': [200, 201], 'date': ['2024-01-01', '2024-01-02']})
        }
        
        combined = extractor.combine_batch_results(
            results=results,
            add_item_column=True,
            item_column_name='symbol'
        )
        
        assert isinstance(combined, pd.DataFrame)
        assert len(combined) == 4
        assert 'symbol' in combined.columns
        assert set(combined['symbol'].unique()) == {'AAPL', 'MSFT'}