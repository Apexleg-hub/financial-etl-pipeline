# tests/test_forex_extractor.py
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.extract.forex_extractor import ForexExtractor
import time


class TestForexExtractor:
    @pytest.fixture
    def extractor(self):
        # Mock the config loading in BaseExtractor
        with patch('src.extract.forex_extractor.settings') as mock_settings:
            mock_config = {
                "alphavantage": {
                    "base_url": "https://www.alphavantage.co/query"
                }
            }
            mock_settings.load_config.return_value = mock_config
            
            # Create the extractor
            extractor = ForexExtractor(api_key="test_key")
            return extractor
    
    def test_get_daily_forex_success(self, extractor):
        """Test getting daily forex data successfully"""
        mock_response = {
            "Meta Data": {
                "1. Information": "Forex Daily Prices (open, high, low, close)",
                "2. From Symbol": "EUR",
                "3. To Symbol": "USD",
                "4. Output Size": "Compact",
                "5. Last Refreshed": "2024-01-01"
            },
            "Time Series FX (Daily)": {
                "2024-01-01": {
                    "1. open": "1.1000",
                    "2. high": "1.1020",
                    "3. low": "1.0980",
                    "4. close": "1.1010"
                }
            }
        }
        
        # Mock _make_forex_request instead of _make_request
        with patch.object(extractor, '_make_forex_request', return_value=mock_response):
            result = extractor.get_daily_forex("EUR", "USD")
            assert result is not None
            assert "2024-01-01" in result
            assert result["2024-01-01"]["1. open"] == "1.1000"
            assert result["2024-01-01"]["2. high"] == "1.1020"
            assert result["2024-01-01"]["3. low"] == "1.0980"
            assert result["2024-01-01"]["4. close"] == "1.1010"
    
    def test_get_exchange_rate_failure(self, extractor):
        """Test when API returns error or no data"""
        # Mock _make_forex_request to return None
        with patch.object(extractor, '_make_forex_request', return_value=None):
            result = extractor.get_exchange_rate("USD", "EUR")
            assert result is None
        
        # Mock empty response
        with patch.object(extractor, '_make_forex_request', return_value={}):
            result = extractor.get_exchange_rate("USD", "EUR")
            assert result is None
    
    def test_get_daily_forex_with_full_output(self, extractor):
        """Test daily forex with full output size"""
        mock_response = {
            "Meta Data": {
                "1. Information": "Forex Daily Prices (open, high, low, close)",
                "2. From Symbol": "EUR",
                "3. To Symbol": "USD",
                "4. Output Size": "Full",
                "5. Last Refreshed": "2024-01-01"
            },
            "Time Series FX (Daily)": {
                "2024-01-01": {"1. open": "1.1000", "2. high": "1.1020", "3. low": "1.0980", "4. close": "1.1010"},
                "2023-12-31": {"1. open": "1.0990", "2. high": "1.1010", "3. low": "1.0970", "4. close": "1.1000"}
            }
        }
        
        with patch.object(extractor, '_make_forex_request', return_value=mock_response):
            result = extractor.get_daily_forex("EUR", "USD", output_size="full")
            assert result is not None
            assert len(result) == 2
            assert "2024-01-01" in result
            assert "2023-12-31" in result
    
    def test_get_weekly_forex_success(self, extractor):
        """Test getting weekly forex data"""
        mock_response = {
            "Meta Data": {
                "1. Information": "Forex Weekly Prices (open, high, low, close)",
                "2. From Symbol": "EUR",
                "3. To Symbol": "USD",
                "4. Last Refreshed": "2024-01-01"
            },
            "Time Series FX (Weekly)": {
                "2024-01-05": {
                    "1. open": "1.1000",
                    "2. high": "1.1020",
                    "3. low": "1.0980",
                    "4. close": "1.1010"
                }
            }
        }
        
        with patch.object(extractor, '_make_forex_request', return_value=mock_response):
            result = extractor.get_weekly_forex("EUR", "USD")
            assert result is not None
            assert "2024-01-05" in result
            assert result["2024-01-05"]["1. open"] == "1.1000"
    
    def test_get_monthly_forex_success(self, extractor):
        """Test getting monthly forex data"""
        mock_response = {
            "Meta Data": {
                "1. Information": "Forex Monthly Prices (open, high, low, close)",
                "2. From Symbol": "EUR",
                "3. To Symbol": "USD",
                "4. Last Refreshed": "2024-01-01"
            },
            "Time Series FX (Monthly)": {
                "2024-01-31": {
                    "1. open": "1.1000",
                    "2. high": "1.1020",
                    "3. low": "1.0980",
                    "4. close": "1.1010"
                }
            }
        }
        
        with patch.object(extractor, '_make_forex_request', return_value=mock_response):
            result = extractor.get_monthly_forex("EUR", "USD")
            assert result is not None
            assert "2024-01-31" in result
    
    def test_get_multiple_timeframes(self, extractor):
        """Test getting multiple timeframes for a currency pair"""
        # Mock responses for different timeframes
        daily_response = {
            "Meta Data": {"1. Information": "Forex Daily Prices"},
            "Time Series FX (Daily)": {"2024-01-01": {"1. open": "1.1000", "4. close": "1.1010"}}
        }
        
        weekly_response = {
            "Meta Data": {"1. Information": "Forex Weekly Prices"},
            "Time Series FX (Weekly)": {"2024-01-05": {"1. open": "1.1050", "4. close": "1.1060"}}
        }
        
        # Mock _make_forex_request to return different responses based on params
        def mock_make_forex_request(params):
            if params.get("function") == "FX_DAILY":
                return daily_response
            elif params.get("function") == "FX_WEEKLY":
                return weekly_response
            return None
        
        with patch.object(extractor, '_make_forex_request', side_effect=mock_make_forex_request):
            result = extractor.get_multiple_timeframes("EUR", "USD", ["daily", "weekly"])
            
            assert "daily" in result
            assert "weekly" in result
            assert result["daily"] is not None
            assert result["weekly"] is not None
            assert "2024-01-01" in result["daily"]
            assert "2024-01-05" in result["weekly"]
    
    def test_get_forex_batch(self, extractor):
        """Test batch extraction for multiple currency pairs"""
        mock_responses = [
            {
                "Meta Data": {"1. Information": "Forex Daily Prices", "2. From Symbol": "EUR", "3. To Symbol": "USD"},
                "Time Series FX (Daily)": {"2024-01-01": {"1. open": "1.1000", "4. close": "1.1010"}}
            },
            {
                "Meta Data": {"1. Information": "Forex Daily Prices", "2. From Symbol": "GBP", "3. To Symbol": "USD"},
                "Time Series FX (Daily)": {"2024-01-01": {"1. open": "1.2700", "4. close": "1.2710"}}
            }
        ]
        
        with patch.object(extractor, '_make_forex_request', side_effect=mock_responses):
            pairs = [("EUR", "USD"), ("GBP", "USD")]
            result = extractor.get_forex_batch(pairs, ["daily"])
            
            assert "EUR_USD" in result
            assert "GBP_USD" in result
            assert "daily" in result["EUR_USD"]
            assert "daily" in result["GBP_USD"]
            assert result["EUR_USD"]["daily"] is not None
            assert result["GBP_USD"]["daily"] is not None
    
    def test_extract_and_transform(self, extractor):
        """Test extraction with transformation"""
        mock_response = {
            "Meta Data": {
                "1. Information": "Forex Daily Prices",
                "2. From Symbol": "EUR",
                "3. To Symbol": "USD",
                "4. Output Size": "Compact",
                "5. Last Refreshed": "2024-01-01"
            },
            "Time Series FX (Daily)": {
                "2024-01-01": {
                    "1. open": "1.1000",
                    "2. high": "1.1020",
                    "3. low": "1.0980",
                    "4. close": "1.1010"
                }
            }
        }
        
        with patch.object(extractor, '_make_forex_request', return_value=mock_response):
            result = extractor.extract_and_transform("EUR", "USD", ["daily"])
            
            assert "daily" in result
            assert len(result["daily"]) == 1
            
            record = result["daily"][0]
            assert record["currency_pair"] == "EURUSD"
            assert record["date"] == "2024-01-01"
            assert record["timeframe"] == "daily"
            assert record["open"] == 1.1000
            assert record["high"] == 1.1020
            assert record["low"] == 1.0980
            assert record["close"] == 1.1010
            assert "metadata" in record
    
    def test_rate_limiting(self, extractor):
        """Test that rate limiting is called"""
        # Mock the rate limiting method
        with patch.object(extractor, '_rate_limit') as mock_rate_limit:
            with patch.object(extractor, '_make_forex_request', return_value={}):
                extractor.get_daily_forex("EUR", "USD")
                
                # Verify rate limiting was called
                mock_rate_limit.assert_called_once()
    
    def test_invalid_timeframe(self, extractor):
        """Test error handling for invalid timeframe"""
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            extractor.extract_and_transform("EUR", "USD", ["invalid"])
    
    def test_intraday_forex(self, extractor):
        """Test intraday forex data extraction"""
        mock_response = {
            "Meta Data": {
                "1. Information": "FX Intraday (5min) open, high, low, close prices",
                "2. From Symbol": "EUR",
                "3. To Symbol": "USD",
                "4. Last Refreshed": "2024-01-01 16:00:00",
                "5. Interval": "5min"
            },
            "Time Series FX (5min)": {
                "2024-01-01 16:00:00": {
                    "1. open": "1.1000",
                    "2. high": "1.1020",
                    "3. low": "1.0980",
                    "4. close": "1.1010"
                }
            }
        }
        
        # Mock the _make_forex_request method
        with patch.object(extractor, '_make_forex_request', return_value=mock_response):
            result = extractor.get_intraday_forex("EUR", "USD", interval="5min")
            
            assert result is not None
            assert "2024-01-01 16:00:00" in result
            assert result["2024-01-01 16:00:00"]["1. open"] == "1.1000"
    
    def test_intraday_invalid_interval(self, extractor):
        """Test error for invalid intraday interval"""
        with pytest.raises(ValueError, match="Interval must be one of"):
            extractor.get_intraday_forex("EUR", "USD", interval="2min")


if __name__ == "__main__":
    # Run tests directly if needed
    pytest.main([__file__, "-v"])