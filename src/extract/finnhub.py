# src/extract/finnhub.py
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import pytz
from .base_extractor import BaseExtractor
from ..config.settings import settings
from ..utils.logger import logger
from ..utils.rate_limiter import RateLimitConfig, rate_limiter


class FinnhubExtractor(BaseExtractor):
    """Finnhub API extractor for financial data"""
    
    def __init__(self):
        super().__init__("finnhub")
        source_config = self.config["sources"]["finnhub"]
        self._base_url = source_config["base_url"]
        self.endpoints = source_config["endpoints"]
        
        # Register rate limit
        rate_config = RateLimitConfig(
            max_requests=source_config["rate_limit"],
            time_window=60,
            retry_delay=60
        )
        rate_limiter.register_source(self.source_name, rate_config)
    
    @property
    def api_key(self) -> str:
        return settings.finnhub_api_key
    
    @property
    def base_url(self) -> str:
        return self._base_url
    
    def extract_stock_quote(
        self,
        symbol: str
    ) -> pd.DataFrame:
        """
        Extract real-time stock quote
        
        Args:
            symbol: Stock symbol
        
        Returns:
            DataFrame with quote data
        """
        endpoint = f"{self.endpoints['quote']}"
        params = {
            "symbol": symbol,
            "token": self.api_key
        }
        
        logger.info(f"Extracting stock quote for {symbol}", symbol=symbol)
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if not data or 'c' not in data:
            logger.warning(f"No quote data returned for {symbol}", symbol=symbol)
            return pd.DataFrame()
        
        quote_data = {
            "symbol": symbol,
            "timestamp": datetime.utcnow(),
            "current_price": data.get('c', 0),
            "change": data.get('d', 0),
            "percent_change": data.get('dp', 0),
            "high_price": data.get('h', 0),
            "low_price": data.get('l', 0),
            "open_price": data.get('o', 0),
            "previous_close": data.get('pc', 0),
            "volume": data.get('v', 0)
        }
        
        return pd.DataFrame([quote_data])
    
    def extract_company_profile(
        self,
        symbol: str
    ) -> pd.DataFrame:
        """
        Extract company profile information
        
        Args:
            symbol: Stock symbol
        
        Returns:
            DataFrame with company profile
        """
        endpoint = f"{self.endpoints['company_profile']}"
        params = {
            "symbol": symbol,
            "token": self.api_key
        }
        
        logger.info(f"Extracting company profile for {symbol}", symbol=symbol)
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if not data or 'name' not in data:
            logger.warning(f"No profile data returned for {symbol}", symbol=symbol)
            return pd.DataFrame()
        
        profile_data = {
            "symbol": symbol,
            "company_name": data.get('name', ''),
            "exchange": data.get('exchange', ''),
            "currency": data.get('currency', 'USD'),
            "country": data.get('country', ''),
            "industry": data.get('finnhubIndustry', ''),
            "market_cap": data.get('marketCapitalization', 0),
            "share_outstanding": data.get('shareOutstanding', 0),
            "web_url": data.get('weburl', ''),
            "logo_url": data.get('logo', ''),
            "ipo_date": pd.to_datetime(data.get('ipo', '')) if data.get('ipo') else None,
            "extracted_at": datetime.utcnow()
        }
        
        return pd.DataFrame([profile_data])
    
    def extract_economic_calendar(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Extract economic calendar events
        
        Args:
            start_date: Start date for events
            end_date: End date for events
        
        Returns:
            DataFrame with economic calendar events
        """
        endpoint = f"{self.endpoints['economic_calendar']}"
        
        if start_date is None:
            start_date = datetime.utcnow() - timedelta(days=7)
        if end_date is None:
            end_date = datetime.utcnow() + timedelta(days=30)
        
        params = {
            "from": start_date.strftime('%Y-%m-%d'),
            "to": end_date.strftime('%Y-%m-%d'),
            "token": self.api_key
        }
        
        logger.info(
            "Extracting economic calendar",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat()
        )
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if not data or 'economicCalendar' not in data:
            logger.warning("No economic calendar data returned")
            return pd.DataFrame()
        
        events = []
        for event in data['economicCalendar']:
            event_data = {
                "event_id": event.get('id'),
                "country": event.get('country'),
                "category": event.get('category'),
                "event": event.get('event'),
                "reference": event.get('reference'),
                "source": event.get('source'),
                "source_url": event.get('sourceURL'),
                "actual": event.get('actual'),
                "previous": event.get('previous'),
                "forecast": event.get('forecast'),
                "unit": event.get('unit'),
                "importance": event.get('importance'),
                "event_date": pd.to_datetime(event.get('date'), utc=True) if event.get('date') else None,
                "extracted_at": datetime.utcnow()
            }
            events.append(event_data)
        
        return pd.DataFrame(events)
    
    def extract_stock_candles(
        self,
        symbol: str,
        resolution: str = 'D',
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Extract stock candle data (OHLCV)
        
        Args:
            symbol: Stock symbol
            resolution: Data resolution (1, 5, 15, 30, 60, D, W, M)
            start_date: Start timestamp
            end_date: End timestamp
        
        Returns:
            DataFrame with candle data
        """
        endpoint = "/stock/candle"
        
        if start_date is None:
            start_date = datetime.utcnow() - timedelta(days=365)
        if end_date is None:
            end_date = datetime.utcnow()
        
        # Convert to Unix timestamps
        from_timestamp = int(start_date.timestamp())
        to_timestamp = int(end_date.timestamp())
        
        params = {
            "symbol": symbol,
            "resolution": resolution,
            "from": from_timestamp,
            "to": to_timestamp,
            "token": self.api_key
        }
        
        logger.info(
            f"Extracting stock candles for {symbol}",
            symbol=symbol,
            resolution=resolution,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat()
        )
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if data.get('s') != 'ok' or 't' not in data:
            logger.warning(f"No candle data returned for {symbol}", symbol=symbol)
            return pd.DataFrame()
        
        candles = []
        for i in range(len(data['t'])):
            candle_data = {
                "symbol": symbol,
                "timestamp": pd.to_datetime(data['t'][i], unit='s', utc=True),
                "open": data['o'][i],
                "high": data['h'][i],
                "low": data['l'][i],
                "close": data['c'][i],
                "volume": data.get('v', [])[i] if 'v' in data else 0,
                "resolution": resolution,
                "extracted_at": datetime.utcnow()
            }
            candles.append(candle_data)
        
        return pd.DataFrame(candles)
    
    def extract_market_news(
        self,
        category: str = "general",
        min_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Extract market news
        
        Args:
            category: News category (general, forex, crypto, merger)
            min_id: Minimum news ID for pagination
        
        Returns:
            DataFrame with market news
        """
        endpoint = "/news"
        
        params = {
            "category": category,
            "token": self.api_key
        }
        
        if min_id:
            params["minId"] = min_id
        
        logger.info(f"Extracting market news", category=category, min_id=min_id)
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if not data:
            logger.warning(f"No news data returned for category {category}")
            return pd.DataFrame()
        
        news_items = []
        for item in data:
            news_data = {
                "news_id": item.get('id'),
                "category": category,
                "datetime": pd.to_datetime(item.get('datetime'), unit='s', utc=True) if item.get('datetime') else None,
                "headline": item.get('headline', ''),
                "summary": item.get('summary', ''),
                "source": item.get('source', ''),
                "url": item.get('url', ''),
                "related": item.get('related', ''),
                "image": item.get('image', ''),
                "lang": item.get('lang', 'en'),
                "has_paywall": item.get('hasPaywall', False),
                "extracted_at": datetime.utcnow()
            }
            news_items.append(news_data)
        
        return pd.DataFrame(news_items)
    
    def _parse_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse Finnhub API response into DataFrame.
        This is a generic parser that handles different response types.
        """
        # This method is implemented in specific extraction methods
        # Return empty DataFrame as fallback
        return pd.DataFrame()
    
    def extract(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        incremental_field: str = "timestamp",
        last_extracted: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Override base extract method for Finnhub-specific logic
        """
        try:
            # Add API token to params
            if params is None:
                params = {}
            params["token"] = self.api_key
            
            response = self._make_request(endpoint, params)
            data = response.json()
            
            # Parse based on endpoint
            df = self._parse_endpoint_response(endpoint, data)
            
            # Apply incremental filtering
            if last_extracted and incremental_field in df.columns:
                df = df[df[incremental_field] > last_extracted]
                logger.info(
                    f"Extracted {len(df)} new records from {self.source_name}",
                    source=self.source_name,
                    total_records=len(df),
                    last_extracted=last_extracted.isoformat()
                )
            
            return df
            
        except Exception as e:
            logger.error(
                f"Extraction failed for {self.source_name}",
                exc_info=e,
                source=self.source_name,
                endpoint=endpoint
            )
            raise
    
    def _parse_endpoint_response(self, endpoint: str, data: Dict[str, Any]) -> pd.DataFrame:
        """Parse response based on endpoint"""
        # This is handled by specific methods above
        return pd.DataFrame()