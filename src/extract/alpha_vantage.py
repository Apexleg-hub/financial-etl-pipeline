
# src/extract/alpha_vantage.py
import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime
from .base_extractor import BaseExtractor
from config.settings import settings
from ..utils.logger import logger


class AlphaVantageExtractor(BaseExtractor):
    """Alpha Vantage API extractor"""
    
    def __init__(self):
        super().__init__("alpha_vantage")
        source_config = self.config["sources"]["alpha_vantage"]
        self._base_url = source_config["base_url"]
        self.endpoints = source_config["endpoints"]
        
        # Register rate limit
        from ..utils.rate_limiter import RateLimitConfig, rate_limiter
        rate_config = RateLimitConfig(
            max_requests=source_config["rate_limit"],
            time_window=60,
            retry_delay=source_config["retry_delay"]
        )
        rate_limiter.register_source(self.source_name, rate_config)
    
    @property
    def api_key(self) -> str:
        return settings.alpha_vantage_api_key
    
    @property
    def base_url(self) -> str:
        return self._base_url
    
    def extract_stock_daily(
        self,
        symbol: str,
        output_size: str = "compact"
    ) -> pd.DataFrame:
        """Extract daily stock prices"""
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": output_size,
            "datatype": "json"
        }
        
        last_extracted = self.get_last_extracted_date(symbol)
        
        df = self.extract(
            endpoint="",
            params=params,
            incremental_field="date",
            last_extracted=last_extracted
        )
        
        return df
    
    def _parse_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Parse Alpha Vantage response"""
        if "Error Message" in data:
            raise ValueError(f"API Error: {data['Error Message']}")
        
        if "Time Series (Daily)" in data:
            time_series = data["Time Series (Daily)"]
            records = []
            
            for date_str, values in time_series.items():
                record = {
                    "date": pd.to_datetime(date_str),
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"]),
                    "symbol": data.get("Meta Data", {}).get("2. Symbol", "")
                }
                records.append(record)
            
            return pd.DataFrame(records)
        
        elif "Time Series (60min)" in data or "Time Series (30min)" in data:
            # Handle intraday data
            for key in data.keys():
                if "Time Series" in key:
                    time_series = data[key]
                    break
            
            records = []
            for timestamp_str, values in time_series.items():
                record = {
                    "timestamp": pd.to_datetime(timestamp_str),
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"])
                }
                records.append(record)
            
            return pd.DataFrame(records)
        
        else:
            raise ValueError(f"Unexpected response format: {list(data.keys())}")