# src/extract/forex_extractor.py
from typing import Dict, Any, Optional
import requests
from .base_extractor import BaseExtractor
from src.utils.logger import logger

class ForexExtractor(BaseExtractor):
    """Extractor for Forex data from Alpha Vantage"""
    
    def __init__(self, api_key: str):
        super().__init__(api_key)
        self.base_url = "https://www.alphavantage.co/query"
    
    def get_exchange_rate(
        self,
        from_currency: str,
        to_currency: str
    ) -> Optional[Dict[str, Any]]:
        """Get real-time exchange rate"""
        params = {
            "function": "CURRENCY_EXCHANGE_RATE",
            "from_currency": from_currency.upper(),
            "to_currency": to_currency.upper(),
            "apikey": self.api_key
        }
        
        try:
            response = self._make_request(params)
            if response and "Realtime Currency Exchange Rate" in response:
                return response["Realtime Currency Exchange Rate"]
            return None
        except Exception as e:
            logger.error(f"Error getting exchange rate: {str(e)}", exc_info=e)
            return None
    
    def get_daily_forex(
        self,
        from_symbol: str,
        to_symbol: str,
        output_size: str = "compact"
    ) -> Optional[Dict[str, Any]]:
        """Get daily Forex data"""
        params = {
            "function": "FX_DAILY",
            "from_symbol": from_symbol.upper(),
            "to_symbol": to_symbol.upper(),
            "outputsize": output_size,
            "apikey": self.api_key
        }
        
        try:
            response = self._make_request(params)
            return self._parse_time_series_response(response, "Time Series FX (Daily)")
        except Exception as e:
            logger.error(f"Error getting daily Forex: {str(e)}", exc_info=e)
            return None
    
    def get_intraday_forex(
        self,
        from_symbol: str,
        to_symbol: str,
        interval: str = "5min",
        output_size: str = "compact"
    ) -> Optional[Dict[str, Any]]:
        """Get intraday Forex data"""
        valid_intervals = ["1min", "5min", "15min", "30min", "60min"]
        if interval not in valid_intervals:
            raise ValueError(f"Interval must be one of {valid_intervals}")
        
        params = {
            "function": "FX_INTRADAY",
            "from_symbol": from_symbol.upper(),
            "to_symbol": to_symbol.upper(),
            "interval": interval,
            "outputsize": output_size,
            "apikey": self.api_key
        }
        
        try:
            response = self._make_request(params)
            return self._parse_time_series_response(response, f"Time Series FX ({interval})")
        except Exception as e:
            logger.error(f"Error getting intraday Forex: {str(e)}", exc_info=e)
            return None
    
    def get_weekly_forex(
        self,
        from_symbol: str,
        to_symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get weekly Forex data"""
        params = {
            "function": "FX_WEEKLY",
            "from_symbol": from_symbol.upper(),
            "to_symbol": to_symbol.upper(),
            "apikey": self.api_key
        }
        
        try:
            response = self._make_request(params)
            return self._parse_time_series_response(response, "Time Series FX (Weekly)")
        except Exception as e:
            logger.error(f"Error getting weekly Forex: {str(e)}", exc_info=e)
            return None
    
    def get_monthly_forex(
        self,
        from_symbol: str,
        to_symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get monthly Forex data"""
        params = {
            "function": "FX_MONTHLY",
            "from_symbol": from_symbol.upper(),
            "to_symbol": to_symbol.upper(),
            "apikey": self.api_key
        }
        
        try:
            response = self._make_request(params)
            return self._parse_time_series_response(response, "Time Series FX (Monthly)")
        except Exception as e:
            logger.error(f"Error getting monthly Forex: {str(e)}", exc_info=e)
            return None