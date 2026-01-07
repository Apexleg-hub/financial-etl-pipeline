# src/extract/weather.py
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import requests
from .base_extractor import BaseExtractor
from config.settings import settings
from ..utils.logger import logger
from ..utils.rate_limiter import RateLimitConfig, rate_limiter


class WeatherExtractor(BaseExtractor):
    """Weather data extractor for OpenWeatherMap and other weather APIs"""
    
    def __init__(self, source: str = "openweather"):
        """
        Initialize weather extractor
        
        Args:
            source: Weather data source (openweather, weatherbit, visualcrossing)
        """
        super().__init__(f"weather_{source}")
        self.source = source.lower()
        
        source_config = self.config["sources"]["weather"]
        self._base_url = source_config["base_url"]
        self.endpoints = source_config["endpoints"]
        
        # Register rate limit
        rate_config = RateLimitConfig(
            max_requests=source_config["rate_limit"],
            time_window=60,
            retry_delay=60
        )
        rate_limiter.register_source(self.source_name, rate_config)
        
        # Source-specific API key
        if self.source == "openweather":
            self.api_key_name = "openweather_api_key"
        elif self.source == "weatherbit":
            self.api_key_name = "weatherbit_api_key"
        elif self.source == "visualcrossing":
            self.api_key_name = "visualcrossing_api_key"
        else:
            raise ValueError(f"Unsupported weather source: {source}")
    
    @property
    def api_key(self) -> str:
        """Get API key from settings"""
        return getattr(settings, self.api_key_name, "")
    
    @property
    def base_url(self) -> str:
        return self._base_url
    
    def extract_current_weather(
        self,
        location: str,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        units: str = "metric"
    ) -> pd.DataFrame:
        """
        Extract current weather data
        
        Args:
            location: Location name (city, country)
            lat: Latitude (optional if location is provided)
            lon: Longitude (optional if location is provided)
            units: Temperature units (metric, imperial, standard)
        
        Returns:
            DataFrame with current weather data
        """
        if lat is not None and lon is not None:
            # Use coordinates if provided
            endpoint = "/weather"
            params = {
                "lat": lat,
                "lon": lon,
                "units": units,
                "appid": self.api_key
            }
            location_str = f"({lat}, {lon})"
        else:
            # Use location name
            endpoint = "/weather"
            params = {
                "q": location,
                "units": units,
                "appid": self.api_key
            }
            location_str = location
        
        logger.info(
            f"Extracting current weather for {location_str}",
            source=self.source,
            location=location,
            lat=lat,
            lon=lon,
            units=units
        )
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if data.get("cod") != 200:
            logger.warning(
                f"Weather API error: {data.get('message', 'Unknown error')}",
                source=self.source,
                location=location,
                error_code=data.get('cod')
            )
            return pd.DataFrame()
        
        # Parse current weather data
        weather_data = {
            "location": location,
            "latitude": data.get("coord", {}).get("lat"),
            "longitude": data.get("coord", {}).get("lon"),
            "timestamp": pd.to_datetime(data.get("dt"), unit='s', utc=True),
            "temperature": data.get("main", {}).get("temp"),
            "feels_like": data.get("main", {}).get("feels_like"),
            "temp_min": data.get("main", {}).get("temp_min"),
            "temp_max": data.get("main", {}).get("temp_max"),
            "pressure": data.get("main", {}).get("pressure"),
            "humidity_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent": data.get("main", {}).get("humidity_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent"),
            "sea_level": data.get("main", {}).get("sea_level"),
            "grnd_level": data.get("main", {}).get("grnd_level"),
            "visibility": data.get("visibility"),
            "wind_speed": data.get("wind", {}).get("speed"),
            "wind_direction": data.get("wind", {}).get("deg"),
            "wind_gust": data.get("wind", {}).get("gust"),
            "cloudiness": data.get("clouds", {}).get("all"),
            "rain_1h": data.get("rain", {}).get("1h") if data.get("rain") else None,
            "rain_3h": data.get("rain", {}).get("3h") if data.get("rain") else None,
            "snow_1h": data.get("snow", {}).get("1h") if data.get("snow") else None,
            "snow_3h": data.get("snow", {}).get("3h") if data.get("snow") else None,
            "weather_main": data.get("weather", [{}])[0].get("main") if data.get("weather") else None,
            "weather_description": data.get("weather", [{}])[0].get("description") if data.get("weather") else None,
            "weather_icon": data.get("weather", [{}])[0].get("icon") if data.get("weather") else None,
            "sunrise": pd.to_datetime(data.get("sys", {}).get("sunrise"), unit='s', utc=True) if data.get("sys", {}).get("sunrise") else None,
            "sunset": pd.to_datetime(data.get("sys", {}).get("sunset"), unit='s', utc=True) if data.get("sys", {}).get("sunset") else None,
            "timezone": data.get("timezone"),
            "country": data.get("sys", {}).get("country"),
            "city_id": data.get("id"),
            "city_name": data.get("name"),
            "units": units,
            "source": self.source,
            "extracted_at": datetime.utcnow()
        }
        
        return pd.DataFrame([weather_data])
    
    def extract_forecast(
        self,
        location: str,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        days: int = 5,
        units: str = "metric"
    ) -> pd.DataFrame:
        """
        Extract weather forecast data
        
        Args:
            location: Location name
            lat: Latitude
            lon: Longitude
            days: Number of forecast days (1-16)
            units: Temperature units
        
        Returns:
            DataFrame with forecast data
        """
        if lat is not None and lon is not None:
            endpoint = "/forecast"
            params = {
                "lat": lat,
                "lon": lon,
                "cnt": days * 8,  # 8 forecasts per day (3-hour intervals)
                "units": units,
                "appid": self.api_key
            }
            location_str = f"({lat}, {lon})"
        else:
            endpoint = "/forecast"
            params = {
                "q": location,
                "cnt": days * 8,
                "units": units,
                "appid": self.api_key
            }
            location_str = location
        
        logger.info(
            f"Extracting {days}-day forecast for {location_str}",
            source=self.source,
            location=location,
            days=days,
            units=units
        )
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if data.get("cod") != "200":
            logger.warning(
                f"Forecast API error: {data.get('message', 'Unknown error')}",
                source=self.source,
                location=location,
                error_code=data.get('cod')
            )
            return pd.DataFrame()
        
        forecasts = []
        city_data = data.get("city", {})
        
        for forecast in data.get("list", []):
            forecast_data = {
                "location": location,
                "latitude": city_data.get("coord", {}).get("lat"),
                "longitude": city_data.get("coord", {}).get("lon"),
                "country": city_data.get("country"),
                "timezone": city_data.get("timezone"),
                "timestamp": pd.to_datetime(forecast.get("dt"), unit='s', utc=True),
                "temperature": forecast.get("main", {}).get("temp"),
                "feels_like": forecast.get("main", {}).get("feels_like"),
                "temp_min": forecast.get("main", {}).get("temp_min"),
                "temp_max": forecast.get("main", {}).get("temp_max"),
                "pressure": forecast.get("main", {}).get("pressure"),
                "sea_level": forecast.get("main", {}).get("sea_level"),
                "grnd_level": forecast.get("main", {}).get("grnd_level"),
                "humidity_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent": forecast.get("main", {}).get("humidity_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent"),
                "visibility": forecast.get("visibility"),
                "wind_speed": forecast.get("wind", {}).get("speed"),
                "wind_direction": forecast.get("wind", {}).get("deg"),
                "wind_gust": forecast.get("wind", {}).get("gust"),
                "cloudiness": forecast.get("clouds", {}).get("all"),
                "pop": forecast.get("pop"),  # Probability of precipitation
                "rain_3h": forecast.get("rain", {}).get("3h") if forecast.get("rain") else None,
                "snow_3h": forecast.get("snow", {}).get("3h") if forecast.get("snow") else None,
                "weather_main": forecast.get("weather", [{}])[0].get("main") if forecast.get("weather") else None,
                "weather_description": forecast.get("weather", [{}])[0].get("description") if forecast.get("weather") else None,
                "weather_icon": forecast.get("weather", [{}])[0].get("icon") if forecast.get("weather") else None,
                "forecast_type": "3hour",
                "units": units,
                "source": self.source,
                "extracted_at": datetime.utcnow()
            }
            forecasts.append(forecast_data)
        
        return pd.DataFrame(forecasts)
    
    def extract_daily_forecast(
        self,
        location: str,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        days: int = 7,
        units: str = "metric"
    ) -> pd.DataFrame:
        """
        Extract daily weather forecast (One Call API 3.0)
        
        Args:
            location: Location name
            lat: Latitude
            lon: Longitude
            days: Number of forecast days (1-16)
            units: Temperature units
        
        Returns:
            DataFrame with daily forecast data
        """
        if lat is None or lon is None:
            # Get coordinates from location name
            coords = self._get_coordinates(location)
            if not coords:
                logger.error(f"Could not get coordinates for location: {location}")
                return pd.DataFrame()
            lat, lon = coords
        
        # Use One Call API 3.0
        endpoint = "/onecall"
        params = {
            "lat": lat,
            "lon": lon,
            "exclude": "current,minutely,hourly,alerts",
            "units": units,
            "appid": self.api_key
        }
        
        logger.info(
            f"Extracting {days}-day daily forecast for ({lat}, {lon})",
            source=self.source,
            location=location,
            lat=lat,
            lon=lon,
            days=days
        )
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if "daily" not in data:
            logger.warning(
                f"Daily forecast API error",
                source=self.source,
                location=location,
                response_keys=list(data.keys())
            )
            return pd.DataFrame()
        
        daily_forecasts = []
        for day_forecast in data.get("daily", [])[:days]:  # Limit to requested days
            daily_data = {
                "location": location,
                "latitude": lat,
                "longitude": lon,
                "date": pd.to_datetime(day_forecast.get("dt"), unit='s', utc=True).date(),
                "timestamp": pd.to_datetime(day_forecast.get("dt"), unit='s', utc=True),
                "sunrise": pd.to_datetime(day_forecast.get("sunrise"), unit='s', utc=True) if day_forecast.get("sunrise") else None,
                "sunset": pd.to_datetime(day_forecast.get("sunset"), unit='s', utc=True) if day_forecast.get("sunset") else None,
                "moonrise": pd.to_datetime(day_forecast.get("moonrise"), unit='s', utc=True) if day_forecast.get("moonrise") else None,
                "moonset": pd.to_datetime(day_forecast.get("moonset"), unit='s', utc=True) if day_forecast.get("moonset") else None,
                "moon_phase": day_forecast.get("moon_phase"),
                "temp_day": day_forecast.get("temp", {}).get("day"),
                "temp_min": day_forecast.get("temp", {}).get("min"),
                "temp_max": day_forecast.get("temp", {}).get("max"),
                "temp_night": day_forecast.get("temp", {}).get("night"),
                "temp_eve": day_forecast.get("temp", {}).get("eve"),
                "temp_morn": day_forecast.get("temp", {}).get("morn"),
                "feels_like_day": day_forecast.get("feels_like", {}).get("day"),
                "feels_like_night": day_forecast.get("feels_like", {}).get("night"),
                "feels_like_eve": day_forecast.get("feels_like", {}).get("eve"),
                "feels_like_morn": day_forecast.get("feels_like", {}).get("morn"),
                "pressure": day_forecast.get("pressure"),
                "humidity_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent": day_forecast.get("humidity_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent"),
                "dew_point": day_forecast.get("dew_point"),
                "wind_speed": day_forecast.get("wind_speed"),
                "wind_direction": day_forecast.get("wind_deg"),
                "wind_gust": day_forecast.get("wind_gust"),
                "cloudiness": day_forecast.get("clouds"),
                "uvi": day_forecast.get("uvi"),  # UV Index
                "pop": day_forecast.get("pop"),  # Probability of precipitation
                "rain": day_forecast.get("rain"),
                "snow": day_forecast.get("snow"),
                "weather_main": day_forecast.get("weather", [{}])[0].get("main") if day_forecast.get("weather") else None,
                "weather_description": day_forecast.get("weather", [{}])[0].get("description") if day_forecast.get("weather") else None,
                "weather_icon": day_forecast.get("weather", [{}])[0].get("icon") if day_forecast.get("weather") else None,
                "forecast_type": "daily",
                "units": units,
                "source": self.source,
                "extracted_at": datetime.utcnow()
            }
            daily_forecasts.append(daily_data)
        
        return pd.DataFrame(daily_forecasts)
    
    def extract_historical_weather(
        self,
        location: str,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        units: str = "metric"
    ) -> pd.DataFrame:
        """
        Extract historical weather data
        
        Args:
            location: Location name
            lat: Latitude
            lon: Longitude
            start_date: Start date for historical data
            end_date: End date for historical data
            units: Temperature units
        
        Returns:
            DataFrame with historical weather data
        """
        if lat is None or lon is None:
            # Get coordinates from location name
            coords = self._get_coordinates(location)
            if not coords:
                logger.error(f"Could not get coordinates for location: {location}")
                return pd.DataFrame()
            lat, lon = coords
        
        if start_date is None:
            start_date = datetime.utcnow() - timedelta(days=30)
        if end_date is None:
            end_date = datetime.utcnow() - timedelta(days=1)
        
        # Ensure dates are in UTC and at 00:00:00
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        logger.info(
            f"Extracting historical weather for ({lat}, {lon}) from {start_date} to {end_date}",
            source=self.source,
            location=location,
            lat=lat,
            lon=lon,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat()
        )
        
        # Process dates one by one (OpenWeather historical API has daily limits)
        all_historical_data = []
        current_date = start_date
        
        while current_date <= end_date:
            try:
                historical_data = self._extract_single_day_historical(lat, lon, current_date, units)
                if not historical_data.empty:
                    all_historical_data.append(historical_data)
                
                # Add delay to respect rate limits
                import time
                time.sleep(0.5)
                
            except Exception as e:
                logger.warning(
                    f"Failed to extract historical data for {current_date.date()}",
                    exc_info=e,
                    date=current_date.date()
                )
            
            current_date += timedelta(days=1)
        
        if all_historical_data:
            return pd.concat(all_historical_data, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def _extract_single_day_historical(
        self,
        lat: float,
        lon: float,
        date: datetime,
        units: str
    ) -> pd.DataFrame:
        """Extract historical weather for a single day"""
        # Convert to Unix timestamp
        unix_timestamp = int(date.timestamp())
        
        endpoint = "/onecall/timemachine"
        params = {
            "lat": lat,
            "lon": lon,
            "dt": unix_timestamp,
            "units": units,
            "appid": self.api_key
        }
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if "current" not in data:
            logger.warning(
                f"No historical data for {date.date()}",
                date=date.date(),
                response_keys=list(data.keys())
            )
            return pd.DataFrame()
        
        historical_records = []
        current_data = data.get("current", {})
        
        historical_record = {
            "latitude": lat,
            "longitude": lon,
            "timestamp": pd.to_datetime(current_data.get("dt"), unit='s', utc=True),
            "temperature": current_data.get("temp"),
            "feels_like": current_data.get("feels_like"),
            "pressure": current_data.get("pressure"),
            "humidity_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent": current_data.get("humidity_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent"),
            "dew_point": current_data.get("dew_point"),
            "uvi": current_data.get("uvi"),
            "cloudiness": current_data.get("clouds"),
            "visibility": current_data.get("visibility"),
            "wind_speed": current_data.get("wind_speed"),
            "wind_direction": current_data.get("wind_deg"),
            "wind_gust": current_data.get("wind_gust"),
            "weather_main": current_data.get("weather", [{}])[0].get("main") if current_data.get("weather") else None,
            "weather_description": current_data.get("weather", [{}])[0].get("description") if current_data.get("weather") else None,
            "weather_icon": current_data.get("weather", [{}])[0].get("icon") if current_data.get("weather") else None,
            "sunrise": pd.to_datetime(data.get("sunrise"), unit='s', utc=True) if data.get("sunrise") else None,
            "sunset": pd.to_datetime(data.get("sunset"), unit='s', utc=True) if data.get("sunset") else None,
            "data_type": "historical",
            "units": units,
            "source": self.source,
            "extracted_at": datetime.utcnow()
        }
        historical_records.append(historical_record)
        
        # Also get hourly data if available
        for hour_data in data.get("hourly", []):
            hourly_record = {
                "latitude": lat,
                "longitude": lon,
                "timestamp": pd.to_datetime(hour_data.get("dt"), unit='s', utc=True),
                "temperature": hour_data.get("temp"),
                "feels_like": hour_data.get("feels_like"),
                "pressure": hour_data.get("pressure"),
                "humidity_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent": hour_data.get("humidity_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent_percent"),
                "dew_point": hour_data.get("dew_point"),
                "uvi": hour_data.get("uvi"),
                "cloudiness": hour_data.get("clouds"),
                "visibility": hour_data.get("visibility"),
                "wind_speed": hour_data.get("wind_speed"),
                "wind_direction": hour_data.get("wind_deg"),
                "wind_gust": hour_data.get("wind_gust"),
                "pop": hour_data.get("pop"),
                "rain_1h": hour_data.get("rain", {}).get("1h") if hour_data.get("rain") else None,
                "snow_1h": hour_data.get("snow", {}).get("1h") if hour_data.get("snow") else None,
                "weather_main": hour_data.get("weather", [{}])[0].get("main") if hour_data.get("weather") else None,
                "weather_description": hour_data.get("weather", [{}])[0].get("description") if hour_data.get("weather") else None,
                "weather_icon": hour_data.get("weather", [{}])[0].get("icon") if hour_data.get("weather") else None,
                "data_type": "historical_hourly",
                "units": units,
                "source": self.source,
                "extracted_at": datetime.utcnow()
            }
            historical_records.append(hourly_record)
        
        return pd.DataFrame(historical_records)
    
    def extract_air_pollution(
        self,
        lat: float,
        lon: float,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Extract air pollution data
        
        Args:
            lat: Latitude
            lon: Longitude
            start_date: Start date for pollution data
            end_date: End date for pollution data
        
        Returns:
            DataFrame with air pollution data
        """
        if start_date is None:
            start_date = datetime.utcnow() - timedelta(days=7)
        if end_date is None:
            end_date = datetime.utcnow()
        
        # Convert to Unix timestamps
        start_unix = int(start_date.timestamp())
        end_unix = int(end_date.timestamp())
        
        endpoint = "/air_pollution/history"
        params = {
            "lat": lat,
            "lon": lon,
            "start": start_unix,
            "end": end_unix,
            "appid": self.api_key
        }
        
        logger.info(
            f"Extracting air pollution data for ({lat}, {lon})",
            source=self.source,
            lat=lat,
            lon=lon,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat()
        )
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if "list" not in data:
            logger.warning(
                f"No air pollution data returned",
                source=self.source,
                lat=lat,
                lon=lon
            )
            return pd.DataFrame()
        
        pollution_data = []
        for pollution_record in data.get("list", []):
            components = pollution_record.get("components", {})
            record = {
                "latitude": lat,
                "longitude": lon,
                "timestamp": pd.to_datetime(pollution_record.get("dt"), unit='s', utc=True),
                "aqi": pollution_record.get("main", {}).get("aqi"),  # Air Quality Index
                "co": components.get("co"),  # Carbon monoxide
                "no": components.get("no"),  # Nitric oxide
                "no2": components.get("no2"),  # Nitrogen dioxide
                "o3": components.get("o3"),  # Ozone
                "so2": components.get("so2"),  # Sulphur dioxide
                "pm2_5": components.get("pm2_5"),  # Fine particles
                "pm10": components.get("pm10"),  # Coarse particles
                "nh3": components.get("nh3"),  # Ammonia
                "source": self.source,
                "extracted_at": datetime.utcnow()
            }
            pollution_data.append(record)
        
        return pd.DataFrame(pollution_data)
    
    def _get_coordinates(self, location: str) -> Optional[Tuple[float, float]]:
        """
        Get coordinates for a location name
        
        Args:
            location: Location name (city, country)
        
        Returns:
            Tuple of (latitude, longitude) or None
        """
        endpoint = "/geo/1.0/direct"
        params = {
            "q": location,
            "limit": 1,
            "appid": self.api_key
        }
        
        try:
            response = self._make_request(endpoint, params)
            data = response.json()
            
            if data and len(data) > 0:
                lat = data[0].get("lat")
                lon = data[0].get("lon")
                if lat and lon:
                    logger.info(
                        f"Resolved location '{location}' to coordinates ({lat}, {lon})",
                        location=location,
                        lat=lat,
                        lon=lon
                    )
                    return (lat, lon)
            
            logger.warning(f"Could not resolve coordinates for location: {location}")
            return None
            
        except Exception as e:
            logger.error(
                f"Failed to get coordinates for {location}",
                exc_info=e,
                location=location
            )
            return None
    
    def extract_multiple_locations(
        self,
        locations: List[Dict[str, Any]],
        data_type: str = "current"
    ) -> pd.DataFrame:
        """
        Extract weather data for multiple locations
        
        Args:
            locations: List of location dictionaries with 'name', 'lat', 'lon'
            data_type: Type of data to extract (current, forecast, historical)
        
        Returns:
            DataFrame with weather data for all locations
        """
        all_data = []
        
        for location in locations:
            name = location.get("name", "")
            lat = location.get("lat")
            lon = location.get("lon")
            
            try:
                if data_type == "current":
                    df = self.extract_current_weather(name, lat, lon)
                elif data_type == "forecast":
                    df = self.extract_forecast(name, lat, lon)
                elif data_type == "daily_forecast":
                    df = self.extract_daily_forecast(name, lat, lon)
                elif data_type == "historical":
                    df = self.extract_historical_weather(name, lat, lon)
                else:
                    logger.warning(f"Unsupported data type: {data_type}")
                    continue
                
                if not df.empty:
                    all_data.append(df)
                
                # Add delay to respect rate limits
                import time
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(
                    f"Failed to extract {data_type} data for {name}",
                    exc_info=e,
                    location=name,
                    data_type=data_type
                )
                continue
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def _parse_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse weather API response into DataFrame
        """
        # Weather responses vary by endpoint, so this is handled by specific methods
        # Return empty DataFrame as fallback
        return pd.DataFrame()
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET"
    ) -> requests.Response:
        """
        Override to handle weather-specific API requirements
        """
        # Add API key to params
        if params is None:
            params = {}
        
        if 'appid' not in params:
            params['appid'] = self.api_key
        
        return super()._make_request(endpoint, params, method)
    
    def validate_api_key(self) -> bool:
        """
        Validate the API key by making a simple request
        
        Returns:
            True if API key is valid, False otherwise
        """
        try:
            # Try to get weather for a known location (London)
            test_df = self.extract_current_weather("London", units="metric")
            if not test_df.empty and test_df.iloc[0]['temperature'] is not None:
                logger.info("Weather API key validated successfully")
                return True
            else:
                logger.warning("Weather API key validation failed - no data returned")
                return False
        except Exception as e:
            logger.error("Weather API key validation failed", exc_info=e)
            return False