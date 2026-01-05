# src/extract/forex_extractor.py
from typing import Dict, Any, Optional, List, Tuple
import time
import requests
from .base_extractor import BaseExtractor
from src.utils.logger import logger


class ForexExtractor(BaseExtractor):
    """Enhanced Forex data extractor with batch processing and rate limiting"""
    
    def __init__(self, api_key: str):
        super().__init__(api_key)
        self.base_url = "https://www.alphavantage.co/query"
        self.request_count = 0
        self.last_request_time = 0
        self.calls_per_minute = 5  # Alpha Vantage free tier limit
        self.calls_per_day = 500
    
    def _rate_limit(self):
        """Implement rate limiting to respect API limits"""
        current_time = time.time()
        
        # Check if we need to wait before next request
        if self.request_count >= self.calls_per_minute:
            time_since_first = current_time - self.last_request_time
            if time_since_first < 60:
                sleep_time = 60 - time_since_first + 1
                logger.info(f"Rate limiting: sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time)
                self.request_count = 0
        
        self.request_count += 1
        if self.last_request_time == 0:
            self.last_request_time = current_time
    
    def get_exchange_rate(
        self,
        from_currency: str,
        to_currency: str
    ) -> Optional[Dict[str, Any]]:
        """Get real-time exchange rate"""
        self._rate_limit()
        
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
        self._rate_limit()
        
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
    
    def get_weekly_forex(
        self,
        from_symbol: str,
        to_symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get weekly Forex data"""
        self._rate_limit()
        
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
        self._rate_limit()
        
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
    
    # NEW METHODS FOR BATCH PROCESSING
    
    def get_multiple_timeframes(
        self,
        from_symbol: str,
        to_symbol: str,
        timeframes: List[str] = ["daily", "weekly"]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Get multiple timeframes for a currency pair
        
        Args:
            from_symbol: Base currency
            to_symbol: Quote currency
            timeframes: List of timeframes to fetch (daily, weekly, monthly)
        
        Returns:
            Dictionary with timeframe as key and data as value
        """
        results = {}
        
        for timeframe in timeframes:
            if timeframe.lower() == "daily":
                data = self.get_daily_forex(from_symbol, to_symbol)
            elif timeframe.lower() == "weekly":
                data = self.get_weekly_forex(from_symbol, to_symbol)
            elif timeframe.lower() == "monthly":
                data = self.get_monthly_forex(from_symbol, to_symbol)
            else:
                logger.warning(f"Unsupported timeframe: {timeframe}")
                data = None
            
            results[timeframe] = data
        
        return results
    
    def get_forex_batch(
        self,
        currency_pairs: List[Tuple[str, str]],
        timeframes: List[str] = ["daily", "weekly"]
    ) -> Dict[str, Dict[str, Optional[Dict[str, Any]]]]:
        """
        Get Forex data for multiple currency pairs and timeframes
        
        Args:
            currency_pairs: List of (from_symbol, to_symbol) tuples
            timeframes: List of timeframes to fetch
        
        Returns:
            Nested dictionary: {currency_pair: {timeframe: data}}
        """
        results = {}
        
        logger.info(f"Starting batch extraction for {len(currency_pairs)} currency pairs")
        
        for i, (from_symbol, to_symbol) in enumerate(currency_pairs):
            pair_key = f"{from_symbol}_{to_symbol}"
            logger.info(f"Processing pair {i+1}/{len(currency_pairs)}: {pair_key}")
            
            try:
                pair_data = self.get_multiple_timeframes(from_symbol, to_symbol, timeframes)
                results[pair_key] = pair_data
                
                # Log progress
                successful_timeframes = sum(1 for data in pair_data.values() if data is not None)
                logger.info(f"  Extracted {successful_timeframes}/{len(timeframes)} timeframes for {pair_key}")
                
            except Exception as e:
                logger.error(f"Error processing {pair_key}: {str(e)}", exc_info=e)
                results[pair_key] = {tf: None for tf in timeframes}
        
        logger.info(f"Batch extraction completed. Successfully processed {len(results)} pairs")
        return results
    
    def get_forex_with_metadata(
        self,
        from_symbol: str,
        to_symbol: str,
        timeframe: str = "daily"
    ) -> Optional[Dict[str, Any]]:
        """
        Get Forex data with metadata included
        
        Returns:
            {
                "metadata": {...},
                "data": {...}
            }
        """
        self._rate_limit()
        
        if timeframe.lower() == "daily":
            function = "FX_DAILY"
            data_key = "Time Series FX (Daily)"
        elif timeframe.lower() == "weekly":
            function = "FX_WEEKLY"
            data_key = "Time Series FX (Weekly)"
        elif timeframe.lower() == "monthly":
            function = "FX_MONTHLY"
            data_key = "Time Series FX (Monthly)"
        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        
        params = {
            "function": function,
            "from_symbol": from_symbol.upper(),
            "to_symbol": to_symbol.upper(),
            "apikey": self.api_key
        }
        
        try:
            response = self._make_request(params)
            if not response:
                return None
            
            return {
                "metadata": response.get("Meta Data", {}),
                "data": self._parse_time_series_response(response, data_key)
            }
        except Exception as e:
            logger.error(f"Error getting Forex with metadata: {str(e)}", exc_info=e)
            return None
    
    # In src/extract/forex_extractor.py, update the extract_and_transform method:
def extract_and_transform(
    self,
    from_symbol: str,
    to_symbol: str,
    timeframes: List[str] = ["daily", "weekly"]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Extract and transform Forex data into a structured format
    
    Returns:
        Dictionary with timeframe as key and list of transformed records
    """
    results = {}
    
    for timeframe in timeframes:
        raw_data = self.get_forex_with_metadata(from_symbol, to_symbol, timeframe)
        if not raw_data or not raw_data.get("data"):
            results[timeframe] = []
            continue
        
        # Transform the data
        transformed = []
        for date_str, prices in raw_data["data"].items():
            record = {
                "currency_pair": f"{from_symbol}{to_symbol}",
                "date": date_str,
                "timeframe": timeframe,
                "open": float(prices.get("1. open", 0)),
                "high": float(prices.get("2. high", 0)),
                "low": float(prices.get("3. low", 0)),
                "close": float(prices.get("4. close", 0)),
                "metadata": raw_data.get("metadata", {})
            }
            transformed.append(record)
        
        results[timeframe] = transformed
    
    return results

# Helper functions for common operations
def get_major_currency_pairs() -> List[Tuple[str, str]]:
    """Get list of major Forex currency pairs"""
    return [
        ("EUR", "USD"),  # Euro/US Dollar
        ("USD", "JPY"),  # US Dollar/Japanese Yen
        ("GBP", "USD"),  # British Pound/US Dollar
        ("USD", "CHF"),  # US Dollar/Swiss Franc
        ("AUD", "USD"),  # Australian Dollar/US Dollar
        ("USD", "CAD"),  # US Dollar/Canadian Dollar
        ("NZD", "USD"),  # New Zealand Dollar/US Dollar
        ("USD", "CNY"),  # US Dollar/Chinese Yuan
    ]


def get_minor_currency_pairs() -> List[Tuple[str, str]]:
    """Get list of minor Forex currency pairs"""
    return [
        ("EUR", "GBP"),  # Euro/British Pound
        ("EUR", "JPY"),  # Euro/Japanese Yen
        ("GBP", "JPY"),  # British Pound/Japanese Yen
        ("EUR", "CHF"),  # Euro/Swiss Franc
        ("AUD", "JPY"),  # Australian Dollar/Japanese Yen
        ("USD", "SGD"),  # US Dollar/Singapore Dollar
        ("USD", "HKD"),  # US Dollar/Hong Kong Dollar
    ]


# Usage example
if __name__ == "__main__":
    # Example usage
    from config.settings import settings
    
    extractor = ForexExtractor(api_key=settings.alpha_vantage_api_key)
    
    # Get multiple timeframes for EUR/USD
    timeframes_data = extractor.get_multiple_timeframes("EUR", "USD", ["daily", "weekly"])
    print(f"Daily records: {len(timeframes_data['daily'] or {})}")
    print(f"Weekly records: {len(timeframes_data['weekly'] or {})}")
    
    # Batch extraction for multiple pairs
    major_pairs = get_major_currency_pairs()
    batch_data = extractor.get_forex_batch(major_pairs[:3], ["daily", "weekly"])
    
    # Extract and transform data
    transformed = extractor.extract_and_transform("EUR", "USD", ["daily", "weekly"])
    print(f"Transformed daily records: {len(transformed['daily'])}")
    print(f"Transformed weekly records: {len(transformed['weekly'])}")