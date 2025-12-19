# tests/test_extract.py
import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime
from src.extract.alpha_vantage import AlphaVantageExtractor


class TestAlphaVantageExtractor:
    
    @pytest.fixture
    def extractor(self):
        with patch('src.extract.alpha_vantage.settings') as mock_settings:
            mock_settings.alpha_vantage_api_key = "test_key"
            mock_settings.load_config.return_value = {
                "sources": {
                    "alpha_vantage": {
                        "base_url": "https://test.com",
                        "endpoints": {},
                        "rate_limit": 5,
                        "retry_delay": 60
                    }
                }
            }
            return AlphaVantageExtractor()
    
    @patch('requests.Session.get')
    def test_extract_stock_daily_success(self, mock_get, extractor):
        """Test successful stock data extraction"""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "Meta Data": {"2. Symbol": "AAPL"},
            "Time Series (Daily)": {
                "2024-01-01": {
                    "1. open": "150.0",
                    "2. high": "155.0",
                    "3. low": "149.0",
                    "4. close": "152.0",
                    "5. volume": "1000000"
                }
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Test extraction
        result = extractor.extract_stock_daily("AAPL")
        
        # Assertions
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]['symbol'] == "AAPL"
        assert result.iloc[0]['close'] == 152.0
    
    def test_parse_response_invalid(self, extractor):
        """Test parsing invalid response"""
        invalid_data = {"Error Message": "Invalid API call"}
        
        with pytest.raises(ValueError, match="API Error"):
            extractor._parse_response(invalid_data)