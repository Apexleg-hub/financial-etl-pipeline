# tests/test_finnhub.py
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.extract.finnhub import FinnhubExtractor
from datetime import datetime, timedelta
import pandas as pd


class TestFinnhubExtractor:
    @pytest.fixture
    def extractor(self):
        """Create a Finnhub extractor instance with mocked config and settings"""
        # Mock config
        mock_config = {
            "sources": {
                "finnhub": {
                    "base_url": "https://finnhub.io/api/v1",
                    "rate_limit": 60,
                    "endpoints": {
                        "quote": "/quote",
                        "company_profile": "/stock/profile2",
                        "economic_calendar": "/economic-calendar",
                        "stock_candles": "/stock/candle",
                        "market_news": "/news"
                    }
                }
            }
        }
        
        with patch('src.extract.finnhub.settings') as mock_settings:
            mock_settings.finnhub_api_key = "test_finnhub_key"
            mock_settings.load_config.return_value = mock_config["sources"]
            
            with patch('src.extract.finnhub.rate_limiter'):
                extractor = FinnhubExtractor()
                return extractor
    
    def test_extract_stock_quote_success(self, extractor):
        """Test successful stock quote extraction"""
        mock_response = {
            'c': 150.25,
            'd': 2.50,
            'dp': 1.69,
            'h': 151.00,
            'l': 149.50,
            'o': 148.75,
            'pc': 147.75,
            'v': 2500000
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_stock_quote("AAPL")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]['symbol'] == "AAPL"
            assert result.iloc[0]['current_price'] == 150.25
            assert result.iloc[0]['change'] == 2.50
            assert result.iloc[0]['volume'] == 2500000
    
    def test_extract_stock_quote_no_data(self, extractor):
        """Test stock quote extraction with no data"""
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = {}
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_stock_quote("INVALID")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_company_profile_success(self, extractor):
        """Test successful company profile extraction"""
        mock_response = {
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'currency': 'USD',
            'country': 'US',
            'finnhubIndustry': 'Technology',
            'marketCapitalization': 2500000,
            'shareOutstanding': 16400,
            'weburl': 'https://apple.com',
            'logo': 'https://logo.com/apple.png',
            'ipo': '1980-12-12'
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_company_profile("AAPL")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]['symbol'] == "AAPL"
            assert result.iloc[0]['company_name'] == "Apple Inc."
            assert result.iloc[0]['exchange'] == "NASDAQ"
            assert result.iloc[0]['industry'] == "Technology"
            assert result.iloc[0]['market_cap'] == 2500000
    
    def test_extract_company_profile_no_data(self, extractor):
        """Test company profile extraction with no data"""
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = {}
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_company_profile("INVALID")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_economic_calendar_success(self, extractor):
        """Test successful economic calendar extraction"""
        mock_response = {
            'economicCalendar': [
                {
                    'id': '1',
                    'country': 'US',
                    'category': 'Employment',
                    'event': 'Non-Farm Payroll',
                    'reference': '2024-01-05',
                    'source': 'Bureau of Labor Statistics',
                    'sourceURL': 'https://bls.gov',
                    'actual': '227000',
                    'previous': '201000',
                    'forecast': '200000',
                    'unit': 'Thousands',
                    'importance': 3,
                    'date': '2024-01-05 13:30:00'
                }
            ]
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_economic_calendar()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]['country'] == 'US'
            assert result.iloc[0]['event'] == 'Non-Farm Payroll'
            assert result.iloc[0]['actual'] == '227000'
            assert result.iloc[0]['importance'] == 3
    
    def test_extract_economic_calendar_with_date_range(self, extractor):
        """Test economic calendar extraction with custom date range"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)
        
        mock_response = {'economicCalendar': []}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_economic_calendar(start_date, end_date)
            
            # Verify the request was made with correct params
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]
            
            assert params['from'] == '2024-01-01'
            assert params['to'] == '2024-01-31'
            assert isinstance(result, pd.DataFrame)
    
    def test_extract_stock_candles_success(self, extractor):
        """Test successful stock candle extraction"""
        mock_response = {
            's': 'ok',
            't': [1704067200, 1704153600, 1704240000],
            'o': [150.0, 151.0, 152.0],
            'h': [151.5, 152.5, 153.5],
            'l': [149.5, 150.5, 151.5],
            'c': [150.5, 151.5, 152.5],
            'v': [2000000, 2100000, 2200000]
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_stock_candles("AAPL", resolution='D')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            assert result.iloc[0]['symbol'] == "AAPL"
            assert result.iloc[0]['open'] == 150.0
            assert result.iloc[0]['close'] == 150.5
            assert result.iloc[0]['volume'] == 2000000
            assert result.iloc[0]['resolution'] == 'D'
    
    def test_extract_stock_candles_no_data(self, extractor):
        """Test stock candle extraction with no data"""
        mock_response = {'s': 'no_data'}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_stock_candles("INVALID")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_stock_candles_with_date_range(self, extractor):
        """Test stock candle extraction with custom date range"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)
        
        mock_response = {'s': 'ok', 't': [], 'o': [], 'h': [], 'l': [], 'c': []}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_stock_candles("AAPL", start_date=start_date, end_date=end_date)
            
            # Verify the request was made with correct params
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]
            
            assert 'from' in params
            assert 'to' in params
            assert params['symbol'] == 'AAPL'
    
    def test_extract_market_news_success(self, extractor):
        """Test successful market news extraction"""
        mock_response = [
            {
                'id': 1,
                'datetime': 1704067200,
                'headline': 'Apple Inc. reports strong Q4 earnings',
                'summary': 'Apple reported record quarterly revenue...',
                'source': 'Reuters',
                'url': 'https://reuters.com/article',
                'related': 'AAPL,MSFT',
                'image': 'https://image.com/apple.jpg',
                'lang': 'en',
                'hasPaywall': False
            },
            {
                'id': 2,
                'datetime': 1704153600,
                'headline': 'Tech stocks rally on AI optimism',
                'summary': 'Technology sector gains momentum...',
                'source': 'Bloomberg',
                'url': 'https://bloomberg.com/article',
                'related': 'QQQ,AAPL',
                'image': 'https://image.com/tech.jpg',
                'lang': 'en',
                'hasPaywall': True
            }
        ]
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_market_news(category='general')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert result.iloc[0]['headline'] == 'Apple Inc. reports strong Q4 earnings'
            assert result.iloc[0]['source'] == 'Reuters'
            assert result.iloc[0]['category'] == 'general'
            assert result.iloc[1]['has_paywall'] == True
    
    def test_extract_market_news_empty(self, extractor):
        """Test market news extraction with no data"""
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = []
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_market_news(category='crypto')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_market_news_with_pagination(self, extractor):
        """Test market news extraction with pagination"""
        mock_response = []
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_market_news(category='forex', min_id=12345)
            
            # Verify pagination parameter was passed
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]
            
            assert params['minId'] == 12345
            assert isinstance(result, pd.DataFrame)
    
    def test_api_key_property(self, extractor):
        """Test API key property"""
        with patch('src.extract.finnhub.settings.finnhub_api_key', "test_key_123"):
            assert extractor.api_key == "test_key_123"
    
    def test_base_url_property(self, extractor):
        """Test base URL property"""
        assert extractor.base_url == "https://finnhub.io/api/v1"
    
    def test_parse_response_returns_empty_dataframe(self, extractor):
        """Test _parse_response returns empty DataFrame"""
        result = extractor._parse_response({})
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_parse_endpoint_response_returns_empty_dataframe(self, extractor):
        """Test _parse_endpoint_response returns empty DataFrame"""
        result = extractor._parse_endpoint_response("/quote", {})
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
