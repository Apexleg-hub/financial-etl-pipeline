# src/extract/twelve_data/base.py
from typing import Dict, Any, Optional, List
import pandas as pd

from src.utils.logger import logger
from src.utils.rate_limiter import rate_limiter
from config.settings import settings
from ..base_extractor import BaseExtractor, ExtractionError   


class TwelveDataExtractor(BaseExtractor):
    def __init__(self):
        super().__init__(source_name="twelve_data")
        self.config = self._load_source_config("twelve_data")
        self.api_key = settings.get("twelve_data.api_key", "")
        self.base_url = self.config.get("base_url", "https://api.twelvedata.com")
        
        self.default_params = {
            "apikey": self.api_key,
            "format": "json",
            "timezone": settings.timezone
        }
      
        
        # Endpoint mappings
        self.endpoints = {
            "time_series": "/time_series",
            "quote": "/quote",
            "symbols": "/stocks",
            "forex_pairs": "/forex_pairs",
            "cryptocurrencies": "/cryptocurrencies",
            "etfs": "/etfs",
            "indices": "/indices",
            "earnings_calendar": "/earnings_calendar",
            "news": "/news"
        }
    
    @property
    def api_key(self) -> str:
        """
        Get API key from environment
        """
        return self._get_api_key()
    
    @property
    def base_url(self) -> str:
        """
        Get base URL from config
        """
        return self.config.get("base_url", "https://api.twelvedata.com")
    
    def _get_api_key(self) -> str:
        """Get API key from settings"""
        return settings.get("twelve_data.api_key", "")
    
    def _make_twelve_data_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Make request to Twelve Data API with proper error handling
        """
        if params is None:
            params = {}
        
        # Add default parameters
        params.update(self.default_params)
        
        # Make request using parent class method
        response = self._make_request(
            endpoint=endpoint,
            params=params,
            **kwargs
        )
        
        data = response.json()
        
        # Check for Twelve Data specific errors
        if "code" in data and data["code"] != 200:
            error_message = data.get("message", "Unknown error")
            
            # Handle rate limiting
            if data["code"] == 429:
                logger.warning(
                    "Rate limit exceeded for Twelve Data",
                    source=self.source_name,
                    endpoint=endpoint
                )
                raise ExtractionError(f"Rate limit exceeded: {error_message}")
            
            # Handle other API errors
            raise ExtractionError(f"Twelve Data API error: {error_message}")
        
        return data
    
    def _parse_time_series_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse time series response into DataFrame
        
        Args:
            data: JSON response from time series endpoint
            
        Returns:
            DataFrame with OHLCV data
        """
        if "values" not in data:
            raise ExtractionError("No time series data found in response")
        
        # Extract metadata
        meta = data.get("meta", {})
        
        # Convert values to DataFrame
        df = pd.DataFrame(data["values"])
        
        # Convert data types
        numeric_columns = ['open', 'high', 'low', 'close']
        if 'volume' in df.columns:
            numeric_columns.append('volume')
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert datetime
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            df.sort_index(inplace=True)
        
        # Add metadata as columns
        for key, value in meta.items():
            if key not in df.columns:
                df[key] = value
        
        return df
    
    def _parse_symbol_list_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse symbol list response into DataFrame
        
        Args:
            data: JSON response from symbol listing endpoints
            
        Returns:
            DataFrame with symbol information
        """
        if "data" not in data:
            raise ExtractionError("No symbol data found in response")
        
        df = pd.DataFrame(data["data"])
        
        # Ensure consistent column names
        column_mapping = {
            'symbol': 'symbol',
            'name': 'name',
            'currency': 'currency',
            'exchange': 'exchange',
            'country': 'country',
            'type': 'asset_type',
            'mic_code': 'mic_code',
            'access': 'access',
            'isin': 'isin'
        }
        
        # Rename columns
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        return df
    
    def _parse_quote_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse quote response into DataFrame
        
        Args:
            data: JSON response from quote endpoint
            
        Returns:
            DataFrame with single row of quote data
        """
        # Extract relevant fields
        quote_data = {
            'symbol': data.get('symbol', ''),
            'timestamp': pd.Timestamp.now(),
            'open': data.get('open'),
            'high': data.get('high'),
            'low': data.get('low'),
            'close': data.get('close'),
            'volume': data.get('volume'),
            'previous_close': data.get('previous_close'),
            'change': data.get('change'),
            'percent_change': data.get('percent_change'),
            'average_volume': data.get('average_volume'),
            'fifty_two_week': data.get('fifty_two_week', {})
        }
        
        df = pd.DataFrame([quote_data])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return df
    
    def get_available_intervals(self) -> List[str]:
        """
        Get list of available time intervals
        
        Returns:
            List of interval strings
        """
        return [
            "1min", "5min", "15min", "30min", "45min",
            "1h", "2h", "4h", "1day", "1week", "1month"
        ]
    
    def validate_symbol(self, symbol: str) -> bool:
        """
        Validate symbol format
        
        Args:
            symbol: Symbol to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not symbol or not isinstance(symbol, str):
            return False
        
        # Basic validation - can be extended based on asset type
        return len(symbol.strip()) > 0