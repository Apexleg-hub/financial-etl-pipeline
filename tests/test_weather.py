# tests/test_weather.py
import pytest
from unittest.mock import Mock, patch
from src.extract.weather import WeatherExtractor
from datetime import datetime, timedelta
import pandas as pd


class TestWeatherExtractor:
    @pytest.fixture
    def extractor(self):
        """Create a weather extractor instance with mocked config and settings"""
        mock_config = {
            "sources": {
                "weather": {
                    "base_url": "https://api.openweathermap.org/data/2.5",
                    "rate_limit": 60,
                    "endpoints": {
                        "current": "/weather",
                        "forecast": "/forecast",
                        "onecall": "/onecall"
                    }
                }
            }
        }
        
        with patch('src.extract.weather.settings') as mock_settings:
            mock_settings.openweather_api_key = "test_openweather_key"
            mock_settings.load_config.return_value = mock_config["sources"]
            
            with patch('src.extract.weather.rate_limiter'):
                extractor = WeatherExtractor(source="openweather")
                return extractor
    
    def test_extract_current_weather_by_location_success(self, extractor):
        """Test successful current weather extraction by location"""
        mock_response = {
            "cod": 200,
            "coord": {"lat": 51.5074, "lon": -0.1278},
            "dt": 1704067200,
            "main": {
                "temp": 10.5,
                "feels_like": 9.2,
                "temp_min": 9.0,
                "temp_max": 11.0,
                "pressure": 1013,
                "humidity": 72,
                "sea_level": 1013,
                "grnd_level": 1000
            },
            "visibility": 10000,
            "wind": {
                "speed": 5.5,
                "deg": 230,
                "gust": 8.2
            },
            "clouds": {"all": 40},
            "rain": {"1h": 0.2, "3h": 0.5},
            "weather": [{"main": "Clouds", "description": "scattered clouds", "icon": "03d"}],
            "sys": {"sunrise": 1704034800, "sunset": 1704067800, "country": "GB"},
            "timezone": 0,
            "id": 2643743,
            "name": "London"
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_current_weather("London", units="metric")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]['location'] == "London"
            assert result.iloc[0]['temperature'] == 10.5
            assert result.iloc[0]['humidity'] == 72
            assert result.iloc[0]['wind_speed'] == 5.5
            assert result.iloc[0]['weather_main'] == "Clouds"
    
    def test_extract_current_weather_by_coordinates_success(self, extractor):
        """Test successful current weather extraction by coordinates"""
        mock_response = {
            "cod": 200,
            "coord": {"lat": 40.7128, "lon": -74.0060},
            "dt": 1704067200,
            "main": {"temp": 5.2, "humidity": 65},
            "wind": {"speed": 3.2, "deg": 180},
            "weather": [{"main": "Clear", "description": "clear sky"}],
            "clouds": {"all": 0},
            "sys": {"country": "US"},
            "name": "New York"
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_current_weather("New York", lat=40.7128, lon=-74.0060)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]['latitude'] == 40.7128
            assert result.iloc[0]['longitude'] == -74.0060
            assert result.iloc[0]['temperature'] == 5.2
    
    def test_extract_current_weather_api_error(self, extractor):
        """Test current weather extraction with API error"""
        mock_response = {
            "cod": "404",
            "message": "city not found"
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_current_weather("InvalidCity")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_forecast_success(self, extractor):
        """Test successful forecast extraction"""
        mock_response = {
            "cod": "200",
            "city": {
                "coord": {"lat": 51.5074, "lon": -0.1278},
                "country": "GB",
                "timezone": 0
            },
            "list": [
                {
                    "dt": 1704067200,
                    "main": {"temp": 10.5, "humidity": 72},
                    "wind": {"speed": 5.5, "deg": 230},
                    "clouds": {"all": 40},
                    "weather": [{"main": "Clouds", "description": "scattered clouds"}],
                    "pop": 0.1
                },
                {
                    "dt": 1704153600,
                    "main": {"temp": 9.5, "humidity": 70},
                    "wind": {"speed": 4.5, "deg": 240},
                    "clouds": {"all": 50},
                    "weather": [{"main": "Clouds", "description": "overcast clouds"}],
                    "pop": 0.2
                }
            ]
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_forecast("London", days=5)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert result.iloc[0]['location'] == "London"
            assert result.iloc[0]['forecast_type'] == "3hour"
            assert result.iloc[1]['temperature'] == 9.5
    
    def test_extract_forecast_error(self, extractor):
        """Test forecast extraction with API error"""
        mock_response = {
            "cod": "404",
            "message": "city not found"
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_forecast("InvalidCity")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_extract_daily_forecast_success(self, extractor):
        """Test successful daily forecast extraction"""
        mock_response = {
            "daily": [
                {
                    "dt": 1704067200,
                    "sunrise": 1704034800,
                    "sunset": 1704067800,
                    "moonrise": 1704050400,
                    "moonset": 1704085200,
                    "moon_phase": 0.5,
                    "temp": {
                        "day": 10.5,
                        "min": 8.0,
                        "max": 12.0,
                        "night": 7.5,
                        "eve": 9.0,
                        "morn": 8.5
                    },
                    "feels_like": {"day": 9.0, "night": 6.5, "eve": 8.0, "morn": 7.5},
                    "pressure": 1013,
                    "humidity": 72,
                    "dew_point": 5.2,
                    "wind_speed": 5.5,
                    "wind_deg": 230,
                    "clouds": 40,
                    "uvi": 2.5,
                    "pop": 0.1,
                    "rain": 0.5,
                    "weather": [{"main": "Clouds", "description": "scattered clouds"}]
                }
            ]
        }
        
        with patch.object(extractor, '_get_coordinates', return_value=(51.5074, -0.1278)):
            with patch.object(extractor, '_make_request') as mock_request:
                mock_response_obj = Mock()
                mock_response_obj.json.return_value = mock_response
                mock_request.return_value = mock_response_obj
                
                result = extractor.extract_daily_forecast("London", days=7)
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 1
                assert result.iloc[0]['forecast_type'] == "daily"
                assert result.iloc[0]['temp_day'] == 10.5
                assert result.iloc[0]['temp_min'] == 8.0
                assert result.iloc[0]['uvi'] == 2.5
    
    def test_extract_daily_forecast_no_daily_key(self, extractor):
        """Test daily forecast extraction when daily key is missing"""
        mock_response = {}
        
        with patch.object(extractor, '_get_coordinates', return_value=(51.5074, -0.1278)):
            with patch.object(extractor, '_make_request') as mock_request:
                mock_response_obj = Mock()
                mock_response_obj.json.return_value = mock_response
                mock_request.return_value = mock_response_obj
                
                result = extractor.extract_daily_forecast("London")
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 0
    
    def test_extract_air_pollution_success(self, extractor):
        """Test successful air pollution extraction"""
        mock_response = {
            "list": [
                {
                    "dt": 1704067200,
                    "main": {"aqi": 2},
                    "components": {
                        "co": 250.0,
                        "no": 10.5,
                        "no2": 15.2,
                        "o3": 45.0,
                        "so2": 8.5,
                        "pm2_5": 12.3,
                        "pm10": 25.5,
                        "nh3": 5.2
                    }
                }
            ]
        }
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_air_pollution(51.5074, -0.1278)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]['aqi'] == 2
            assert result.iloc[0]['pm2_5'] == 12.3
            assert result.iloc[0]['pm10'] == 25.5
            assert result.iloc[0]['no2'] == 15.2
    
    def test_extract_air_pollution_no_data(self, extractor):
        """Test air pollution extraction with no data"""
        mock_response = {}
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            result = extractor.extract_air_pollution(51.5074, -0.1278)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_get_coordinates_success(self, extractor):
        """Test successful coordinate resolution"""
        mock_response = [
            {
                "lat": 51.5074,
                "lon": -0.1278,
                "name": "London"
            }
        ]
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            coords = extractor._get_coordinates("London")
            
            assert coords is not None
            assert coords == (51.5074, -0.1278)
    
    def test_get_coordinates_not_found(self, extractor):
        """Test coordinate resolution when location not found"""
        mock_response = []
        
        with patch.object(extractor, '_make_request') as mock_request:
            mock_response_obj = Mock()
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj
            
            coords = extractor._get_coordinates("InvalidLocation")
            
            assert coords is None
    
    def test_extract_multiple_locations_success(self, extractor):
        """Test successful extraction for multiple locations"""
        with patch.object(extractor, 'extract_current_weather') as mock_extract:
            # Mock return data for each location
            london_df = pd.DataFrame([{
                'location': 'London',
                'temperature': 10.5,
                'humidity': 72
            }])
            ny_df = pd.DataFrame([{
                'location': 'New York',
                'temperature': 5.2,
                'humidity': 65
            }])
            
            mock_extract.side_effect = [london_df, ny_df]
            
            locations = [
                {"name": "London", "lat": 51.5074, "lon": -0.1278},
                {"name": "New York", "lat": 40.7128, "lon": -74.0060}
            ]
            
            result = extractor.extract_multiple_locations(locations, data_type="current")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert result.iloc[0]['location'] == "London"
            assert result.iloc[1]['location'] == "New York"
    
    def test_extract_multiple_locations_empty(self, extractor):
        """Test extraction for multiple locations with empty results"""
        with patch.object(extractor, 'extract_current_weather') as mock_extract:
            mock_extract.return_value = pd.DataFrame()
            
            locations = [
                {"name": "London", "lat": 51.5074, "lon": -0.1278}
            ]
            
            result = extractor.extract_multiple_locations(locations)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
    
    def test_api_key_property(self, extractor):
        """Test API key property"""
        with patch('src.extract.weather.settings.openweather_api_key', "custom_key"):
            assert extractor.api_key == "custom_key"
    
    def test_base_url_property(self, extractor):
        """Test base URL property"""
        assert "api.openweathermap.org/data/2.5" in extractor.base_url
    
    def test_source_name_openweather(self, extractor):
        """Test source name for OpenWeather"""
        assert extractor.source_name == "weather_openweather"
        assert extractor.source == "openweather"
    
    def test_invalid_source(self):
        """Test initialization with invalid source"""
        with patch('src.extract.weather.settings'):
            with patch('src.extract.weather.rate_limiter'):
                with pytest.raises(ValueError, match="Unsupported weather source"):
                    WeatherExtractor(source="invalid_source")
    
    def test_weatherbit_source(self):
        """Test weatherbit source initialization"""
        with patch('src.extract.weather.settings') as mock_settings:
            mock_settings.weatherbit_api_key = "test_key"
            mock_settings.load_config.return_value = {
                "weather": {
                    "base_url": "https://api.weatherbit.io/v2.0",
                    "rate_limit": 50,
                    "endpoints": {}
                }
            }
            
            with patch('src.extract.weather.rate_limiter'):
                extractor = WeatherExtractor(source="weatherbit")
                assert extractor.source == "weatherbit"
                assert extractor.api_key_name == "weatherbit_api_key"
    
    def test_parse_response_returns_empty_dataframe(self, extractor):
        """Test _parse_response returns empty DataFrame"""
        result = extractor._parse_response({})
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
