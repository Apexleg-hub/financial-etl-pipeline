

# src/extract/twelve_data/time_series.py
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import pandas as pd

from .base import TwelveDataExtractor, ExtractionError
from ...utils.logger import logger


class TwelveDataTimeSeriesExtractor(TwelveDataExtractor):
    """
    Extractor for time series data across all asset types
    """
    
    def __init__(self):
        """
        Initialize time series extractor
        """
        super().__init__()
        
        # Default extraction parameters
        self.default_interval = self.config.get("default_interval", "1day")
        self.max_data_points = self.config.get("max_data_points", 5000)
    
    def extract_time_series(
        self,
        symbol: str,
        interval: str = "1day",
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        output_size: int = 1000,
        exchange: Optional[str] = None,
        country: Optional[str] = None,
        asset_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Extract time series data for any symbol
        
        Args:
            symbol: Symbol to extract (e.g., 'AAPL', 'EUR/USD', 'BTC/USD')
            interval: Time interval (default: 1day)
            start_date: Start date for historical data
            end_date: End date for historical data
            output_size: Number of data points to retrieve
            exchange: Exchange code (for stocks)
            country: Country code (for stocks)
            asset_type: Type of asset (for validation)
            
        Returns:
            DataFrame with time series data
        """
        logger.info(
            f"Extracting time series for {symbol}",
            source=self.source_name,
            symbol=symbol,
            interval=interval,
            output_size=output_size
        )
        
        # Validate inputs
        if not self.validate_symbol(symbol):
            raise ExtractionError(f"Invalid symbol: {symbol}")
        
        if interval not in self.get_available_intervals():
            raise ExtractionError(f"Invalid interval: {interval}")
        
        # Prepare parameters
        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": min(output_size, self.max_data_points)
        }
        
        # Add date parameters if provided
        if start_date:
            if isinstance(start_date, datetime):
                start_date = start_date.strftime("%Y-%m-%d")
            params["start_date"] = start_date
        
        if end_date:
            if isinstance(end_date, datetime):
                end_date = end_date.strftime("%Y-%m-%d")
            params["end_date"] = end_date
        
        # Add optional parameters
        if exchange:
            params["exchange"] = exchange
        if country:
            params["country"] = country
        
        try:
            # Make API request
            data = self._make_twelve_data_request(
                endpoint=self.endpoints["time_series"],
                params=params
            )
            
            # Parse response
            df = self._parse_time_series_response(data)
            
            # Add metadata columns
            if asset_type:
                df['asset_type'] = asset_type
            df['symbol'] = symbol
            df['interval'] = interval
            df['extracted_at'] = datetime.now()
            
            logger.info(
                f"Successfully extracted {len(df)} records for {symbol}",
                source=self.source_name,
                symbol=symbol,
                record_count=len(df),
                date_range=f"{df.index.min()} to {df.index.max()}"
            )
            
            return df
            
        except Exception as e:
            logger.error(
                f"Failed to extract time series for {symbol}",
                exc_info=e,
                source=self.source_name,
                symbol=symbol,
                interval=interval
            )
            raise
    
    def extract_time_series_batch(
        self,
        symbols: List[str],
        interval: str = "1day",
        output_size: int = 1000,
        delay: float = 1.0,
        **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        Extract time series for multiple symbols
        
        Args:
            symbols: List of symbols to extract
            interval: Time interval
            output_size: Number of data points per symbol
            delay: Delay between requests in seconds
            **kwargs: Additional parameters for extract_time_series
            
        Returns:
            Dictionary mapping symbol to DataFrame
        """
        logger.info(
            f"Starting batch time series extraction",
            source=self.source_name,
            symbol_count=len(symbols),
            interval=interval
        )
        
        def extract_single(symbol: str) -> pd.DataFrame:
            return self.extract_time_series(
                symbol=symbol,
                interval=interval,
                output_size=output_size,
                **kwargs
            )
        
        results = self.extract_batch(
            items=symbols,
            extract_fn=extract_single,
            delay=delay,
            continue_on_error=True
        )
        
        successful = sum(1 for df in results.values() if not df.empty)
        logger.info(
            f"Batch time series extraction completed",
            source=self.source_name,
            total_symbols=len(symbols),
            successful=successful,
            failed=len(symbols) - successful
        )
        
        return results
    
    def _parse_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse API response (required by BaseExtractor)
        
        Args:
            data: JSON response from API
            
        Returns:
            DataFrame with parsed data
        """
        return self._parse_time_series_response(data)