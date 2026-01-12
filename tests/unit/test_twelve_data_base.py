

# tests/unit/test_twelve_data_base.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

from src.extract.twelve_data.base import TwelveDataExtractor, ExtractionError


class TestTwelveDataExtractor:
    """Test the base Twelve Data extractor"""
    
    def setup_method(self):
        """Setup for each test"""
        self.extractor = TwelveDataExtractor()
        
    def test_initialization(self):
        """Test extractor initialization"""
        assert self.extractor.source_name == "twelve_data"
        assert hasattr(self.extractor, 'config')
        assert hasattr(self.extractor, 'api_key')
        assert hasattr(self.extractor, 'base_url')
        assert hasattr(self.extractor, 'default_params')
        assert hasattr(self.extractor, 'endpoints')
    
    def test_api_key_property(self):
        """Test API key property"""
        with patch('src.extract.twelve_data.base.settings.get') as mock_get:
            mock_get.return_value = "test_api_key_123"
            extractor = TwelveDataExtractor()
            assert extractor.api_key == "test_api_key_123"
    
    def test_base_url_property(self):
        """Test base URL property"""
        with patch.dict(self.extractor.config, {'base_url': 'https://test.api.com'}):
            assert self.extractor.base_url == "https://test.api.com"
    
    def test_validate_symbol(self):
        """Test symbol validation"""
        assert self.extractor.validate_symbol("AAPL") == True
        assert self.extractor.validate_symbol("EUR/USD") == True
        assert self.extractor.validate_symbol("BTC/USD") == True
        assert self.extractor.validate_symbol("") == False
        assert self.extractor.validate_symbol(None) == False
    
    def test_get_available_intervals(self):
        """Test getting available intervals"""
        intervals = self.extractor.get_available_intervals()
        assert isinstance(intervals, list)
        assert len(intervals) > 0
        assert "1day" in intervals
        assert "1min" in intervals
        assert "1week" in intervals
    
    def test_parse_time_series_response(self):
        """Test parsing time series response"""
        mock_response = {
            "meta": {
                "symbol": "AAPL",
                "interval": "1day",
                "currency": "USD"
            },
            "values": [
                {"datetime": "2024-01-01", "open": "150.0", "high": "152.0", "low": "149.0", "close": "151.0", "volume": "1000000"},
                {"datetime": "2024-01-02", "open": "151.0", "high": "153.0", "low": "150.0", "close": "152.0", "volume": "1100000"}
            ]
        }
        
        df = self.extractor._parse_time_series_response(mock_response)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert df.index.name == "datetime"
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert df['symbol'].iloc[0] == "AAPL"
    
    def test_parse_time_series_response_no_values(self):
        """Test parsing response with no values"""
        mock_response = {"meta": {}, "values": []}
        
        with pytest.raises(ExtractionError, match="No time series data found"):
            self.extractor._parse_time_series_response(mock_response)
    
    def test_parse_symbol_list_response(self):
        """Test parsing symbol list response"""
        mock_response = {
            "data": [
                {"symbol": "AAPL", "name": "Apple Inc.", "currency": "USD", "exchange": "NASDAQ"},
                {"symbol": "MSFT", "name": "Microsoft Corp.", "currency": "USD", "exchange": "NASDAQ"}
            ]
        }
        
        df = self.extractor._parse_symbol_list_response(mock_response)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "symbol" in df.columns
        assert "name" in df.columns
        assert "currency" in df.columns
        assert "exchange" in df.columns
        assert "asset_type" in df.columns
    
    def test_parse_quote_response(self):
        """Test parsing quote response"""
        mock_response = {
            "symbol": "AAPL",
            "open": "150.0",
            "high": "152.0",
            "low": "149.0",
            "close": "151.0",
            "volume": "1000000",
            "previous_close": "149.5",
            "change": "1.5",
            "percent_change": "1.0",
            "average_volume": "950000",
            "fifty_two_week": {"high": "180.0", "low": "120.0"}
        }
        
        df = self.extractor._parse_quote_response(mock_response)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df['symbol'].iloc[0] == "AAPL"
        assert df['close'].iloc[0] == 151.0
        assert df['change'].iloc[0] == "1.5"
        assert isinstance(df['timestamp'].iloc[0], pd.Timestamp)