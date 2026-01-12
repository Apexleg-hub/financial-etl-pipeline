# src/extract/twelve_data/etfs.py
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import pandas as pd

from .time_series import TwelveDataTimeSeriesExtractor
from ...utils.logger import logger


class TwelveDataETFExtractor(TwelveDataTimeSeriesExtractor):
    """
    Extractor for ETF data from Twelve Data
    """
    
    def __init__(self):
        """
        Initialize ETF extractor
        """
        super().__init__()
        
        # ETF-specific configuration
        self.etf_config = self.config.get("etfs", {})
        self.default_etfs = self.etf_config.get("default_etfs", [
            "SPY", "QQQ", "VTI", "IVV", "VOO",
            "ARKK", "XLF", "XLK", "XLE", "XLV"
        ])
    
    def get_etfs_list(self) -> pd.DataFrame:
        """
        Get list of available ETFs
        
        Returns:
            DataFrame with ETF information
        """
        logger.info(
            "Fetching available ETFs",
            source=self.source_name
        )
        
        try:
            data = self._make_twelve_data_request(
                endpoint=self.endpoints["etfs"]
            )
            
            df = self._parse_symbol_list_response(data)
            
            # Add ETF-specific metadata
            df['asset_type'] = 'etf'
            df['extracted_at'] = datetime.now()
            
            logger.info(
                f"Found {len(df)} ETFs",
                source=self.source_name,
                etf_count=len(df)
            )
            
            return df
            
        except Exception as e:
            logger.error(
                "Failed to fetch ETFs list",
                exc_info=e,
                source=self.source_name
            )
            raise
    
    def extract_etf_time_series(
        self,
        symbol: str,
        interval: str = "1day",
        output_size: int = 1000,
        **kwargs
    ) -> pd.DataFrame:
        """
        Extract ETF time series data
        
        Args:
            symbol: ETF symbol
            interval: Time interval
            output_size: Number of data points
            **kwargs: Additional parameters
            
        Returns:
            DataFrame with ETF time series
        """
        logger.info(
            f"Extracting ETF data for {symbol}",
            source=self.source_name,
            symbol=symbol,
            interval=interval
        )
        
        # Extract time series with ETF asset type
        df = self.extract_time_series(
            symbol=symbol,
            interval=interval,
            output_size=output_size,
            asset_type='etf',
            **kwargs
        )
        
        return df
    
    def extract_major_etfs(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Extract data for major ETFs
        
        Args:
            **kwargs: Parameters for extract_etf_time_series
            
        Returns:
            Dictionary mapping ETF symbol to DataFrame
        """
        logger.info(
            "Extracting major ETFs",
            source=self.source_name,
            etfs=self.default_etfs
        )
        
        return self.extract_time_series_batch(
            symbols=self.default_etfs,
            **kwargs
        )


# src/extract/twelve_data/indices.py
class TwelveDataIndexExtractor(TwelveDataTimeSeriesExtractor):
    """
    Extractor for Market Indices data from Twelve Data
    """
    
    def __init__(self):
        """
        Initialize Index extractor
        """
        super().__init__()
        
        # Index-specific configuration
        self.index_config = self.config.get("indices", {})
        self.default_indices = self.index_config.get("default_indices", [
            "DJI",  # Dow Jones
            "IXIC",  # NASDAQ
            "GSPC",  # S&P 500
            "RUT",   # Russell 2000
            "FTSE",  # FTSE 100
            "N225",  # Nikkei 225
            "HSI",   # Hang Seng
            "AXJO"   # ASX 200
        ])
    
    def get_indices_list(self) -> pd.DataFrame:
        """
        Get list of available indices
        
        Returns:
            DataFrame with index information
        """
        logger.info(
            "Fetching available indices",
            source=self.source_name
        )
        
        try:
            data = self._make_twelve_data_request(
                endpoint=self.endpoints["indices"]
            )
            
            df = self._parse_symbol_list_response(data)
            
            # Add index-specific metadata
            df['asset_type'] = 'index'
            df['extracted_at'] = datetime.now()
            
            logger.info(
                f"Found {len(df)} indices",
                source=self.source_name,
                index_count=len(df)
            )
            
            return df
            
        except Exception as e:
            logger.error(
                "Failed to fetch indices list",
                exc_info=e,
                source=self.source_name
            )
            raise
    
    def extract_index_time_series(
        self,
        symbol: str,
        interval: str = "1day",
        output_size: int = 1000,
        **kwargs
    ) -> pd.DataFrame:
        """
        Extract index time series data
        
        Args:
            symbol: Index symbol
            interval: Time interval
            output_size: Number of data points
            **kwargs: Additional parameters
            
        Returns:
            DataFrame with index time series
        """
        logger.info(
            f"Extracting index data for {symbol}",
            source=self.source_name,
            symbol=symbol,
            interval=interval
        )
        
        # Extract time series with index asset type
        df = self.extract_time_series(
            symbol=symbol,
            interval=interval,
            output_size=output_size,
            asset_type='index',
            **kwargs
        )
        
        return df
    
    def extract_major_indices(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Extract data for major indices
        
        Args:
            **kwargs: Parameters for extract_index_time_series
            
        Returns:
            Dictionary mapping index symbol to DataFrame
        """
        logger.info(
            "Extracting major indices",
            source=self.source_name,
            indices=self.default_indices
        )
        
        return self.extract_time_series_batch(
            symbols=self.default_indices,
            **kwargs
        )