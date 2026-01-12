


# src/extract/twelve_data/forex.py
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import pandas as pd
from .etfs_indices import TwelveDataETFExtractor, TwelveDataIndexExtractor

from .time_series import TwelveDataTimeSeriesExtractor
from ...utils.logger import logger


class TwelveDataForexExtractor(TwelveDataTimeSeriesExtractor):
    """
    Extractor for Forex data from Twelve Data
    """
    
    def __init__(self):
        """
        Initialize Forex extractor
        """
        super().__init__()
        
        # Forex-specific configuration
        self.forex_config = self.config.get("forex", {})
        self.default_forex_pairs = self.forex_config.get("default_pairs", [
            "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF",
            "AUD/USD", "USD/CAD", "NZD/USD"
        ])
    
    def get_forex_pairs(self) -> pd.DataFrame:
        """
        Get all available Forex pairs
        
        Returns:
            DataFrame with Forex pair information
        """
        logger.info(
            "Fetching available Forex pairs",
            source=self.source_name
        )
        
        try:
            data = self._make_twelve_data_request(
                endpoint=self.endpoints["forex_pairs"]
            )
            
            df = self._parse_symbol_list_response(data)
            
            # Add Forex-specific metadata
            df['asset_type'] = 'forex'
            df['extracted_at'] = datetime.now()
            
            logger.info(
                f"Found {len(df)} Forex pairs",
                source=self.source_name,
                pair_count=len(df)
            )
            
            return df
            
        except Exception as e:
            logger.error(
                "Failed to fetch Forex pairs",
                exc_info=e,
                source=self.source_name
            )
            raise
    
    def extract_forex_time_series(
        self,
        symbol: str,
        interval: str = "1day",
        output_size: int = 1000,
        **kwargs
    ) -> pd.DataFrame:
        """
        Extract Forex time series data
        
        Args:
            symbol: Forex pair symbol (e.g., 'EUR/USD')
            interval: Time interval
            output_size: Number of data points
            **kwargs: Additional parameters
            
        Returns:
            DataFrame with Forex time series
        """
        logger.info(
            f"Extracting Forex data for {symbol}",
            source=self.source_name,
            symbol=symbol,
            interval=interval
        )
        
        # Validate Forex symbol format
        if '/' not in symbol:
            logger.warning(
                f"Forex symbol {symbol} may be malformed (expected format: 'EUR/USD')",
                source=self.source_name,
                symbol=symbol
            )
        
        # Extract time series with Forex asset type
        df = self.extract_time_series(
            symbol=symbol,
            interval=interval,
            output_size=output_size,
            asset_type='forex',
            **kwargs
        )
        
        # Add Forex-specific calculations
        if not df.empty:
            if 'close' in df.columns:
                df['pct_change'] = df['close'].pct_change() * 100
                
                # Calculate spread if bid/ask available
                if 'bid' in df.columns and 'ask' in df.columns:
                    df['spread'] = df['ask'] - df['bid']
                    df['spread_pips'] = df['spread'] * 10000  # For most Forex pairs
        
        return df
    
    def extract_major_pairs(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Extract data for major Forex pairs
        
        Args:
            **kwargs: Parameters for extract_forex_time_series
            
        Returns:
            Dictionary mapping pair symbol to DataFrame
        """
        logger.info(
            "Extracting major Forex pairs",
            source=self.source_name,
            pairs=self.default_forex_pairs
        )
        
        return self.extract_time_series_batch(
            symbols=self.default_forex_pairs,
            **kwargs
        )
    
    def extract_forex_quote(self, symbol: str) -> pd.DataFrame:
        """
        Extract real-time Forex quote
        
        Args:
            symbol: Forex pair symbol
            
        Returns:
            DataFrame with quote data
        """
        logger.info(
            f"Extracting Forex quote for {symbol}",
            source=self.source_name,
            symbol=symbol
        )
        
        try:
            params = {
                "symbol": symbol,
                "type": "forex"
            }
            
            data = self._make_twelve_data_request(
                endpoint=self.endpoints["quote"],
                params=params
            )
            
            df = self._parse_quote_response(data)
            df['asset_type'] = 'forex'
            df['extracted_at'] = datetime.now()
            
            logger.info(
                f"Extracted Forex quote for {symbol}",
                source=self.source_name,
                symbol=symbol,
                price=df['close'].iloc[0] if not df.empty else None
            )
            
            return df
            
        except Exception as e:
            logger.error(
                f"Failed to extract Forex quote for {symbol}",
                exc_info=e,
                source=self.source_name,
                symbol=symbol
            )
            raise