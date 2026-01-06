# tests/test_fred.py
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.extract.fred import FREDExtractor
from datetime import datetime, timedelta
import pandas as pd


class TestFREDExtractor:
    @pytest.fixture
    def extractor(self):
        """Create a FRED extractor instance with mocked config and settings"""
        # Mock config
        mock_config = {
            "sources": {
                "fred": {
                    "base_url": "https://api.stlouisfed.org/fred",
                    "rate_limit": 120,
                    "endpoints": {
                        "series": "/series/observations",
                        "search": "/series/search",
                        "series_info": "/series"
                    }
                }
            }
        }
        
        with patch('src.extract.fred.settings') as mock_settings:
            mock_settings.fred_api_key = "test_fred_key"
            mock_settings.load_config.return_value = mock_config["sources"]
            
            with patch('src.extract.fred.rate_limiter'):
                extractor = FREDExtractor()
                return extractor
    
    def test_extract_series_success(self, extractor):
        """Test successful series extraction"""
        mock_response = {
            'observations': [
                {
                    'date': '2024-01-01',
                    'value': '150.5',
                    'realtime_start': '2024-01-02',
                    'realtime_end': '2024-01-09'
                },
                {
                    'date': '2024-02-01',
                    'value': '151.2',
                    'realtime_start': '2024-02-02',
                    'realtime_end': '2024-02-09'
                }
            ]
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_series('GDP', 
                                             datetime(2024, 1, 1), 
                                             datetime(2024, 2, 28))
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert result.iloc[0]['series_id'] == 'GDP'
            assert result.iloc[0]['value'] == 150.5
            assert result.iloc[1]['value'] == 151.2
    
    def test_extract_series_with_missing_values(self, extractor):
        """Test series extraction with missing values (dots)"""
        mock_response = {
            'observations': [
                {
                    'date': '2024-01-01',
                    'value': '150.5',
                    'realtime_start': '2024-01-02',
                    'realtime_end': '2024-01-09'
                },
                {
                    'date': '2024-02-01',
                    'value': '.',
                    'realtime_start': '2024-02-02',
                    'realtime_end': '2024-02-09'
                },
                {
                    'date': '2024-03-01',
                    'value': '152.1',
                    'realtime_start': '2024-03-02',
                    'realtime_end': '2024-03-09'
                }
            ]
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_series('GDP')
            
            # Should skip the row with '.' value
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert result.iloc[0]['value'] == 150.5
            assert result.iloc[1]['value'] == 152.1
    
    def test_extract_series_no_observations(self, extractor):
        """Test series extraction with no observations"""
        mock_response = {'observations': []}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_series('INVALID')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_series_missing_observations_key(self, extractor):
        """Test series extraction when observations key is missing"""
        mock_response = {}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_series('INVALID')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_series_with_frequency(self, extractor):
        """Test series extraction with frequency and aggregation"""
        mock_response = {'observations': []}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            extractor.extract_series('GDP', frequency='q', aggregation_method='sum')
            
            # Verify frequency parameters were passed
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]
            
            assert params['frequency'] == 'q'
            assert params['aggregation_method'] == 'sum'
    
    def test_extract_multiple_series_success(self, extractor):
        """Test extraction of multiple series"""
        mock_responses = {
            'GDP': pd.DataFrame([
                {'series_id': 'GDP', 'date': datetime(2024, 1, 1), 'value': 150.5}
            ]),
            'UNRATE': pd.DataFrame([
                {'series_id': 'UNRATE', 'date': datetime(2024, 1, 1), 'value': 3.7}
            ])
        }
        
        with patch.object(extractor, 'extract_series') as mock_extract:
            def side_effect(series_id, *args, **kwargs):
                return mock_responses.get(series_id, pd.DataFrame())
            
            mock_extract.side_effect = side_effect
            
            result = extractor.extract_multiple_series(['GDP', 'UNRATE'])
            
            assert isinstance(result, dict)
            assert 'GDP' in result
            assert 'UNRATE' in result
            assert len(result['GDP']) == 1
            assert len(result['UNRATE']) == 1
    
    def test_extract_multiple_series_with_failure(self, extractor):
        """Test extraction of multiple series with one failing"""
        with patch.object(extractor, 'extract_series') as mock_extract:
            def side_effect(series_id, *args, **kwargs):
                if series_id == 'INVALID':
                    raise Exception("API Error")
                return pd.DataFrame([{'series_id': series_id, 'value': 100}])
            
            mock_extract.side_effect = side_effect
            
            result = extractor.extract_multiple_series(['GDP', 'INVALID'])
            
            assert isinstance(result, dict)
            assert 'GDP' in result
            assert 'INVALID' in result
            assert len(result['GDP']) == 1
            assert len(result['INVALID']) == 0
    
    def test_search_series_success(self, extractor):
        """Test successful series search"""
        mock_response = {
            'seriess': [
                {
                    'id': 'GDP',
                    'realtime_start': '2024-01-01',
                    'realtime_end': '2024-01-09',
                    'title': 'Real Gross Domestic Product',
                    'observation_start': '1992-01-01',
                    'observation_end': '2024-01-01',
                    'frequency': 'Quarterly',
                    'frequency_short': 'Q',
                    'units': 'Billions of Chained 2012 Dollars',
                    'units_short': 'Bil. of Chn. 2012 $',
                    'seasonal_adjustment': 'Seasonally Adjusted Annual Rate',
                    'seasonal_adjustment_short': 'SAAR',
                    'last_updated': '2024-01-09',
                    'popularity': 92,
                    'group_popularity': 98,
                    'notes': 'Real GDP'
                }
            ]
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.search_series('GDP', limit=1)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]['series_id'] == 'GDP'
            assert result.iloc[0]['title'] == 'Real Gross Domestic Product'
            assert result.iloc[0]['popularity'] == 92
    
    def test_search_series_no_results(self, extractor):
        """Test series search with no results"""
        mock_response = {'seriess': []}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.search_series('NONEXISTENT')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_search_series_missing_results_key(self, extractor):
        """Test series search when seriess key is missing"""
        mock_response = {}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.search_series('test')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_search_series_with_custom_params(self, extractor):
        """Test series search with custom parameters"""
        mock_response = {'seriess': []}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            extractor.search_series('test', limit=50, order_by='title', sort_order='asc')
            
            # Verify custom parameters were passed
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]
            
            assert params['limit'] == 50
            assert params['order_by'] == 'title'
            assert params['sort_order'] == 'asc'
    
    def test_extract_series_info_success(self, extractor):
        """Test successful series info extraction"""
        mock_response = {
            'seriess': [
                {
                    'id': 'GDP',
                    'realtime_start': '2024-01-01',
                    'realtime_end': '2024-01-09',
                    'title': 'Real Gross Domestic Product',
                    'observation_start': '1992-01-01',
                    'observation_end': '2024-01-01',
                    'frequency': 'Quarterly',
                    'frequency_short': 'Q',
                    'units': 'Billions of Chained 2012 Dollars',
                    'units_short': 'Bil. of Chn. 2012 $',
                    'seasonal_adjustment': 'Seasonally Adjusted Annual Rate',
                    'seasonal_adjustment_short': 'SAAR',
                    'last_updated': '2024-01-09',
                    'popularity': 92,
                    'notes': 'Real GDP'
                }
            ]
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_series_info('GDP')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]['series_id'] == 'GDP'
            assert result.iloc[0]['title'] == 'Real Gross Domestic Product'
            assert result.iloc[0]['frequency'] == 'Quarterly'
    
    def test_extract_series_info_not_found(self, extractor):
        """Test series info extraction with no results"""
        mock_response = {'seriess': []}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_series_info('INVALID')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_series_info_missing_key(self, extractor):
        """Test series info extraction when seriess key is missing"""
        mock_response = {}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_series_info('GDP')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_category_series_success(self, extractor):
        """Test successful category series extraction"""
        mock_response = {
            'seriess': [
                {
                    'id': 'GDP',
                    'title': 'Real Gross Domestic Product',
                    'frequency': 'Quarterly',
                    'units': 'Billions of Chained 2012 Dollars',
                    'seasonal_adjustment': 'Seasonally Adjusted Annual Rate',
                    'realtime_start': '2024-01-01',
                    'realtime_end': '2024-01-09',
                    'observation_start': '1992-01-01',
                    'observation_end': '2024-01-01',
                    'popularity': 92
                },
                {
                    'id': 'GDPC1',
                    'title': 'Real Gross Domestic Product Per Capita',
                    'frequency': 'Quarterly',
                    'units': 'Chained 2012 Dollars',
                    'seasonal_adjustment': 'Seasonally Adjusted Annual Rate',
                    'realtime_start': '2024-01-01',
                    'realtime_end': '2024-01-09',
                    'observation_start': '1992-01-01',
                    'observation_end': '2024-01-01',
                    'popularity': 85
                }
            ]
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_category_series(32991)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert result.iloc[0]['series_id'] == 'GDP'
            assert result.iloc[0]['category_id'] == 32991
            assert result.iloc[1]['series_id'] == 'GDPC1'
    
    def test_extract_category_series_no_results(self, extractor):
        """Test category series extraction with no results"""
        mock_response = {'seriess': []}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_category_series(99999)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_category_series_missing_key(self, extractor):
        """Test category series extraction when seriess key is missing"""
        mock_response = {}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_category_series(32991)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_category_series_with_custom_limit(self, extractor):
        """Test category series extraction with custom limit"""
        mock_response = {'seriess': []}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            extractor.extract_category_series(32991, limit=500)
            
            # Verify limit parameter was passed
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]
            
            assert params['limit'] == 500
    
    def test_api_key_property(self, extractor):
        """Test API key property"""
        with patch('src.extract.fred.settings.fred_api_key', "test_key_123"):
            assert extractor.api_key == "test_key_123"
    
    def test_base_url_property(self, extractor):
        """Test base URL property"""
        assert extractor.base_url == "https://api.stlouisfed.org/fred"
    
    def test_parse_response_returns_empty_dataframe(self, extractor):
        """Test _parse_response returns empty DataFrame"""
        result = extractor._parse_response({})
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_make_request_adds_file_type(self, extractor):
        """Test _make_request adds file_type parameter"""
        with patch('src.extract.fred.BaseExtractor._make_request') as mock_parent:
            extractor._make_request('/series/observations', {})
            
            # Verify file_type was added to params
            call_args = mock_parent.call_args
            params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]
            
            assert params['file_type'] == 'json'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
