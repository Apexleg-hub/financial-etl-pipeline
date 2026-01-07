# src/extract/crypto.py
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import time
import hashlib
import hmac
import requests
from .base_extractor import BaseExtractor
from config.settings import settings
from ..utils.logger import logger
from ..utils.rate_limiter import RateLimitConfig, rate_limiter


class CryptoExtractor(BaseExtractor):
    """Cryptocurrency exchange data extractor"""
    
    def __init__(self, exchange: str = "binance"):
        """
        Initialize cryptocurrency extractor
        
        Args:
            exchange: Exchange name (binance, coinbase, etc.)
        """
        super().__init__(f"crypto_{exchange}")
        self.exchange = exchange.lower()
        
        source_config = self.config["sources"]["crypto"]
        exchange_config = source_config.get("exchanges", {}).get(self.exchange, {})
        
        if not exchange_config:
            raise ValueError(f"Unsupported exchange: {exchange}")
        
        self._base_url = exchange_config.get("base_url", "")
        self.endpoints = exchange_config.get("endpoints", {})
        
        # Register rate limit
        rate_config = RateLimitConfig(
            max_requests=source_config["rate_limit"],
            time_window=60,
            retry_delay=source_config.get("retry_delay", 60)
        )
        rate_limiter.register_source(self.source_name, rate_config)
        
        # Exchange-specific configuration
        self.api_key = getattr(settings, f"{self.exchange}_api_key", "")
        self.api_secret = getattr(settings, f"{self.exchange}_api_secret", "")
    
    @property
    def base_url(self) -> str:
        return self._base_url
    
    @property
    def api_key(self) -> str:
        """Get API key for the exchange"""
        if self.exchange == "binance":
            return self.api_key
        elif self.exchange == "coinbase":
            return self.api_key
        else:
            return ""
    
    def _parse_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Parse API response into DataFrame"""
        # Base implementation - can be overridden if needed
        return pd.DataFrame()
    
    def extract_klines(
        self,
        symbol: str,
        interval: str = "1d",
        start_time: Optional[datetime] = None,
        ended_at: Optional[datetime] = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Extract candlestick (kline) data
        
        Args:
            symbol: Trading pair (e.g., BTCUSDT, ETHUSDT)
            interval: Kline interval (1m, 5m, 15m, 1h, 4h, 1d, etc.)
            start_time: Start timestamp
            ended_at: End timestamp
            limit: Number of klines to fetch
        
        Returns:
            DataFrame with kline data
        """
        if self.exchange == "binance":
            return self._extract_binance_klines(symbol, interval, start_time, ended_at, limit)
        elif self.exchange == "coinbase":
            return self._extract_coinbase_candles(symbol, interval, start_time, ended_at, limit)
        else:
            raise ValueError(f"Unsupported exchange for klines: {self.exchange}")
    
    def _extract_binance_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[datetime],
        ended_at: Optional[datetime],
        limit: int
    ) -> pd.DataFrame:
        """Extract klines from Binance"""
        endpoint = "/api/v3/klines"
        
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit
        }
        
        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if ended_at:
            params["endTime"] = int(ended_at.timestamp() * 1000)
        
        logger.info(
            f"Extracting Binance klines for {symbol}",
            exchange=self.exchange,
            symbol=symbol,
            interval=interval,
            limit=limit
        )
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if isinstance(data, dict) and 'code' in data:
            logger.error(
                f"Binance API error: {data.get('msg', 'Unknown error')}",
                exchange=self.exchange,
                symbol=symbol,
                error_code=data.get('code')
            )
            return pd.DataFrame()
        
        klines = []
        for kline in data:
            kline_data = {
                "symbol": symbol.upper(),
                "exchange": self.exchange,
                "open_time": pd.to_datetime(kline[0], unit='ms', utc=True),
                "open": float(kline[1]),
                "high": float(kline[2]),
                "low": float(kline[3]),
                "close": float(kline[4]),
                "volume": float(kline[5]),
                "close_time": pd.to_datetime(kline[6], unit='ms', utc=True),
                "quote_asset_volume": float(kline[7]),
                "number_of_trades": int(kline[8]),
                "taker_buy_base_asset_volume": float(kline[9]),
                "taker_buy_quote_asset_volume": float(kline[10]),
                "ignore": kline[11],
                "interval": interval,
                "extracted_at": datetime.utcnow()
            }
            klines.append(kline_data)
        
        return pd.DataFrame(klines)
    
    def _extract_coinbase_candles(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[datetime],
        ended_at: Optional[datetime],
        limit: int
    ) -> pd.DataFrame:
        """Extract candles from Coinbase"""
        # Map interval to Coinbase granularity
        granularity_map = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "6h": 21600,
            "1d": 86400
        }
        
        if interval not in granularity_map:
            raise ValueError(f"Unsupported interval for Coinbase: {interval}")
        
        endpoint = f"/products/{symbol.upper()}/candles"
        
        params = {
            "granularity": granularity_map[interval]
        }
        
        if start_time:
            params["start"] = start_time.isoformat()
        if ended_at:
            params["end"] = ended_at.isoformat()
        
        logger.info(
            f"Extracting Coinbase candles for {symbol}",
            exchange=self.exchange,
            symbol=symbol,
            interval=interval,
            granularity=granularity_map[interval]
        )
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if isinstance(data, dict) and 'message' in data:
            logger.error(
                f"Coinbase API error: {data.get('message', 'Unknown error')}",
                exchange=self.exchange,
                symbol=symbol
            )
            return pd.DataFrame()
        
        candles = []
        for candle in data:
            # Coinbase returns [time, low, high, open, close, volume]
            candle_data = {
                "symbol": symbol.upper(),
                "exchange": self.exchange,
                "timestamp": pd.to_datetime(candle[0], unit='s', utc=True),
                "low": float(candle[1]),
                "high": float(candle[2]),
                "open": float(candle[3]),
                "close": float(candle[4]),
                "volume": float(candle[5]),
                "interval": interval,
                "extracted_at": datetime.utcnow()
            }
            candles.append(candle_data)
        
        return pd.DataFrame(candles)
    
    def extract_ticker(
        self,
        symbol: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Extract ticker/price data
        
        Args:
            symbol: Trading pair (optional, returns all if None)
        
        Returns:
            DataFrame with ticker data
        """
        if self.exchange == "binance":
            return self._extract_binance_ticker(symbol)
        elif self.exchange == "coinbase":
            return self._extract_coinbase_ticker(symbol)
        else:
            raise ValueError(f"Unsupported exchange for ticker: {self.exchange}")
    
    def _extract_binance_ticker(self, symbol: Optional[str]) -> pd.DataFrame:
        """Extract ticker from Binance"""
        if symbol:
            endpoint = "/api/v3/ticker/24hr"
            params = {"symbol": symbol.upper()}
        else:
            endpoint = "/api/v3/ticker/24hr"
            params = {}
        
        logger.info(
            f"Extracting Binance ticker for {symbol if symbol else 'all symbols'}",
            exchange=self.exchange,
            symbol=symbol
        )
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if isinstance(data, dict) and 'code' in data:
            logger.error(
                f"Binance API error: {data.get('msg', 'Unknown error')}",
                exchange=self.exchange,
                symbol=symbol
            )
            return pd.DataFrame()
        
        # Handle both single ticker and array of tickers
        if isinstance(data, dict):
            data = [data]
        
        tickers = []
        for ticker in data:
            ticker_data = {
                "symbol": ticker.get('symbol', ''),
                "exchange": self.exchange,
                "price_change": float(ticker.get('priceChange', 0)),
                "price_change_percent": float(ticker.get('priceChangePercent', 0)),
                "weighted_avg_price": float(ticker.get('weightedAvgPrice', 0)),
                "prev_close_price": float(ticker.get('prevClosePrice', 0)),
                "last_price": float(ticker.get('lastPrice', 0)),
                "bid_price": float(ticker.get('bidPrice', 0)),
                "ask_price": float(ticker.get('askPrice', 0)),
                "open_price": float(ticker.get('openPrice', 0)),
                "high_price": float(ticker.get('highPrice', 0)),
                "low_price": float(ticker.get('lowPrice', 0)),
                "volume": float(ticker.get('volume', 0)),
                "quote_volume": float(ticker.get('quoteVolume', 0)),
                "open_time": pd.to_datetime(ticker.get('openTime', 0), unit='ms', utc=True),
                "close_time": pd.to_datetime(ticker.get('closeTime', 0), unit='ms', utc=True),
                "first_id": ticker.get('firstId', 0),
                "last_id": ticker.get('lastId', 0),
                "count": ticker.get('count', 0),
                "extracted_at": datetime.utcnow()
            }
            tickers.append(ticker_data)
        
        return pd.DataFrame(tickers)
    
    def _extract_coinbase_ticker(self, symbol: Optional[str]) -> pd.DataFrame:
        """Extract ticker from Coinbase"""
        if symbol:
            endpoint = f"/products/{symbol.upper()}/ticker"
        else:
            endpoint = "/products"
            # First get all products
            products_response = self._make_request("/products", {})
            products = products_response.json()
            
            tickers = []
            for product in products[:50]:  # Limit to 50 products to avoid rate limits
                try:
                    product_id = product.get('id')
                    if product_id:
                        ticker_df = self._extract_coinbase_ticker(product_id)
                        if not ticker_df.empty:
                            tickers.append(ticker_df)
                        time.sleep(0.1)  # Rate limiting
                except Exception as e:
                    logger.warning(
                        f"Failed to extract ticker for {product.get('id')}",
                        exc_info=e,
                        exchange=self.exchange,
                        product_id=product.get('id')
                    )
                    continue
            
            if tickers:
                return pd.concat(tickers, ignore_index=True)
            else:
                return pd.DataFrame()
        
        logger.info(
            f"Extracting Coinbase ticker for {symbol}",
            exchange=self.exchange,
            symbol=symbol
        )
        
        response = self._make_request(endpoint, {})
        data = response.json()
        
        if 'message' in data:
            logger.error(
                f"Coinbase API error: {data.get('message', 'Unknown error')}",
                exchange=self.exchange,
                symbol=symbol
            )
            return pd.DataFrame()
        
        ticker_data = {
            "symbol": symbol.upper(),
            "exchange": self.exchange,
            "trade_id": data.get('trade_id', 0),
            "price": float(data.get('price', 0)),
            "size": float(data.get('size', 0)),
            "bid": float(data.get('bid', 0)),
            "ask": float(data.get('ask', 0)),
            "volume": float(data.get('volume', 0)),
            "time": pd.to_datetime(data.get('time'), utc=True) if data.get('time') else None,
            "extracted_at": datetime.utcnow()
        }
        
        return pd.DataFrame([ticker_data])