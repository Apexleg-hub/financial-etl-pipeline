

# src/extract/twelve_data/stocks.py
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import pandas as pd

from .time_series import TwelveDataTimeSeriesExtractor
from ...utils.logger import logger


class TwelveDataStockExtractor(TwelveDataTimeSeriesExtractor):
    """
    Extractor for Stock data from Twelve Data
    """
    
    def __init__(self):
        """
        Initialize Stock extractor
        """
        super().__init__()
        
        # Stock-specific configuration
        self.stock_config = self.config.get("stocks", {})
        self.default_stocks = self.stock_config.get("default_stocks", [
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
            "META", "NVDA", "JPM", "JNJ", "V"
        ])
    
    def get_stocks_list(
        self,
        exchange: Optional[str] = None,
        country: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get list of available stocks
        
        Args:
            exchange: Filter by exchange code
            country: Filter by country code
            
        Returns:
            DataFrame with stock information
        """
        logger.info(
            "Fetching available stocks",
            source=self.source_name,
            exchange=exchange,
            country=country
        )
        
        params = {}
        if exchange:
            params["exchange"] = exchange
        if country:
            params["country"] = country
        
        try:
            data = self._make_twelve_data_request(
                endpoint=self.endpoints["symbols"],
                params=params
            )
            
            df = self._parse_symbol_list_response(data)
            
            # Add stock-specific metadata
            df['asset_type'] = 'stock'
            df['extracted_at'] = datetime.now()
            
            logger.info(
                f"Found {len(df)} stocks",
                source=self.source_name,
                stock_count=len(df),
                exchange=exchange,
                country=country
            )
            
            return df
            
        except Exception as e:
            logger.error(
                "Failed to fetch stocks list",
                exc_info=e,
                source=self.source_name,
                exchange=exchange,
                country=country
            )
            raise
    
    def extract_stock_time_series(
        self,
        symbol: str,
        interval: str = "1day",
        output_size: int = 1000,
        exchange: Optional[str] = None,
        country: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Extract stock time series data
        
        Args:
            symbol: Stock symbol
            interval: Time interval
            output_size: Number of data points
            exchange: Exchange code
            country: Country code
            **kwargs: Additional parameters
            
        Returns:
            DataFrame with stock time series
        """
        logger.info(
            f"Extracting stock data for {symbol}",
            source=self.source_name,
            symbol=symbol,
            exchange=exchange,
            interval=interval
        )
        
        # Prepare parameters
        params = kwargs.copy()
        if exchange:
            params["exchange"] = exchange
        if country:
            params["country"] = country
        
        # Extract time series with stock asset type
        df = self.extract_time_series(
            symbol=symbol,
            interval=interval,
            output_size=output_size,
            asset_type='stock',
            **params
        )
        
        # Add stock-specific calculations
        if not df.empty:
            if 'close' in df.columns and 'volume' in df.columns:
                df['returns'] = df['close'].pct_change()
                df['dollar_volume'] = df['close'] * df['volume']
                
                # Calculate moving averages
                if len(df) >= 20:
                    df['sma_20'] = df['close'].rolling(window=20).mean()
                if len(df) >= 50:
                    df['sma_50'] = df['close'].rolling(window=50).mean()
        
        return df
    
    def extract_major_stocks(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Extract data for major stocks
        
        Args:
            **kwargs: Parameters for extract_stock_time_series
            
        Returns:
            Dictionary mapping stock symbol to DataFrame
        """
        logger.info(
            "Extracting major stocks",
            source=self.source_name,
            stocks=self.default_stocks
        )
        
        return self.extract_time_series_batch(
            symbols=self.default_stocks,
            **kwargs
        )
    
    def extract_stock_quote(self, symbol: str, exchange: Optional[str] = None) -> pd.DataFrame:
        """
        Extract real-time stock quote
        
        Args:
            symbol: Stock symbol
            exchange: Exchange code
            
        Returns:
            DataFrame with quote data
        """
        logger.info(
            f"Extracting stock quote for {symbol}",
            source=self.source_name,
            symbol=symbol,
            exchange=exchange
        )
        
        try:
            params = {
                "symbol": symbol,
                "type": "stock"
            }
            
            if exchange:
                params["exchange"] = exchange
            
            data = self._make_twelve_data_request(
                endpoint=self.endpoints["quote"],
                params=params
            )
            
            df = self._parse_quote_response(data)
            df['asset_type'] = 'stock'
            df['extracted_at'] = datetime.now()
            
            # Extract additional metrics if available
            if 'fifty_two_week' in df.columns and isinstance(df['fifty_two_week'].iloc[0], dict):
                week_data = df['fifty_two_week'].iloc[0]
                df['week_high'] = week_data.get('high')
                df['week_low'] = week_data.get('low')
                df['week_range'] = week_data.get('range')
                df['week_percent_change'] = week_data.get('percent_change')
            
            logger.info(
                f"Extracted stock quote for {symbol}",
                source=self.source_name,
                symbol=symbol,
                price=df['close'].iloc[0] if not df.empty else None,
                change=df['percent_change'].iloc[0] if not df.empty else None
            )
            
            return df
            
        except Exception as e:
            logger.error(
                f"Failed to extract stock quote for {symbol}",
                exc_info=e,
                source=self.source_name,
                symbol=symbol
            )
            raise