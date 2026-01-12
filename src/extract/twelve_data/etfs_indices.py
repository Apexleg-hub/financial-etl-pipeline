

# src/extract/twelve_data/etfs_indices.py
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time  # Add this import for the market summary function

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
        self.etf_categories = {
            "broad_market": ["SPY", "VTI", "IVV", "VOO"],
            "sector": ["XLF", "XLK", "XLE", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB"],
            "tech": ["QQQ", "XLK"],
            "innovative": ["ARKK", "ARKG", "ARKF"]
        }
    
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
            
            # Classify ETFs by category if possible
            df['category'] = self._classify_etf(df['name'])
            
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
    
    def _classify_etf(self, etf_names: pd.Series) -> pd.Series:
        """
        Classify ETFs into categories based on name
        
        Args:
            etf_names: Series of ETF names
            
        Returns:
            Series of categories
        """
        categories = []
        
        for name in etf_names:
            name_lower = str(name).lower()
            category = "other"
            
            if any(keyword in name_lower for keyword in ['spdr', 'spy', 's&p', 'standard & poor']):
                category = "broad_market"
            elif any(keyword in name_lower for keyword in ['nasdaq', 'qqq', 'invesco qqq']):
                category = "tech"
            elif any(keyword in name_lower for keyword in ['vanguard', 'total stock', 'vti']):
                category = "broad_market"
            elif any(keyword in name_lower for keyword in ['ishares', 'core s&p']):
                category = "broad_market"
            elif any(keyword in name_lower for keyword in ['financial', 'xlf']):
                category = "financial"
            elif any(keyword in name_lower for keyword in ['technology', 'xlk']):
                category = "tech"
            elif any(keyword in name_lower for keyword in ['energy', 'xle']):
                category = "energy"
            elif any(keyword in name_lower for keyword in ['health', 'xlu']):
                category = "healthcare"
            elif any(keyword in name_lower for keyword in ['ark', 'innovation']):
                category = "innovative"
            elif any(keyword in name_lower for keyword in ['dividend', 'income']):
                category = "dividend"
            
            categories.append(category)
        
        return pd.Series(categories, index=etf_names.index)
    
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
        
        # Add ETF-specific metrics
        if not df.empty and 'close' in df.columns and 'volume' in df.columns:
            # Calculate NAV (Net Asset Value) approximation
            df['nav'] = df['close']
            
            # Calculate daily returns
            df['daily_return'] = df['close'].pct_change()
            
            # Calculate volatility
            df['volatility_20d'] = df['daily_return'].rolling(window=20).std() * np.sqrt(252)
            
            # Calculate volume trend
            df['volume_ma_10'] = df['volume'].rolling(window=10).mean()
            df['volume_trend'] = df['volume'] / df['volume_ma_10']
        
        return df
    
    def extract_etfs_by_category(
        self,
        category: str,
        **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        Extract ETFs by category
        
        Args:
            category: ETF category (broad_market, sector, tech, innovative, etc.)
            **kwargs: Parameters for extract_etf_time_series
            
        Returns:
            Dictionary mapping ETF symbol to DataFrame
        """
        logger.info(
            f"Extracting ETFs for category: {category}",
            source=self.source_name,
            category=category
        )
        
        # Get ETFs in category
        if category in self.etf_categories:
            etf_symbols = self.etf_categories[category]
        else:
            logger.warning(
                f"Unknown ETF category: {category}. Using default ETFs.",
                source=self.source_name,
                category=category
            )
            etf_symbols = self.default_etfs
        
        return self.extract_time_series_batch(
            symbols=etf_symbols,
            **kwargs
        )
    
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
    
    def extract_etf_quote(self, symbol: str) -> pd.DataFrame:
        """
        Extract real-time ETF quote
        
        Args:
            symbol: ETF symbol
            
        Returns:
            DataFrame with quote data
        """
        logger.info(
            f"Extracting ETF quote for {symbol}",
            source=self.source_name,
            symbol=symbol
        )
        
        try:
            params = {
                "symbol": symbol,
                "type": "etf"
            }
            
            data = self._make_twelve_data_request(
                endpoint=self.endpoints["quote"],
                params=params
            )
            
            df = self._parse_quote_response(data)
            df['asset_type'] = 'etf'
            df['extracted_at'] = datetime.now()
            
            # Extract ETF-specific information
            meta = data.get('meta', {})
            df['etf_name'] = meta.get('name', '')
            df['currency'] = meta.get('currency', 'USD')
            df['exchange'] = meta.get('exchange', '')
            
            logger.info(
                f"Extracted ETF quote for {symbol}",
                source=self.source_name,
                symbol=symbol,
                price=df['close'].iloc[0] if not df.empty else None,
                name=df['etf_name'].iloc[0] if not df.empty else None
            )
            
            return df
            
        except Exception as e:
            logger.error(
                f"Failed to extract ETF quote for {symbol}",
                exc_info=e,
                source=self.source_name,
                symbol=symbol
            )
            raise


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
            "DJI",   # Dow Jones Industrial Average
            "IXIC",  # NASDAQ Composite
            "GSPC",  # S&P 500
            "RUT",   # Russell 2000
            "FTSE",  # FTSE 100
            "N225",  # Nikkei 225
            "HSI",   # Hang Seng Index
            "AXJO",  # S&P/ASX 200
            "STOXX50E",  # EURO STOXX 50
            "VIX"    # CBOE Volatility Index
        ])
        
        # Index metadata
        self.index_metadata = {
            "DJI": {"name": "Dow Jones Industrial Average", "region": "US", "type": "price_weighted"},
            "IXIC": {"name": "NASDAQ Composite", "region": "US", "type": "market_cap_weighted"},
            "GSPC": {"name": "S&P 500", "region": "US", "type": "market_cap_weighted"},
            "RUT": {"name": "Russell 2000", "region": "US", "type": "market_cap_weighted"},
            "FTSE": {"name": "FTSE 100", "region": "UK", "type": "market_cap_weighted"},
            "N225": {"name": "Nikkei 225", "region": "Japan", "type": "price_weighted"},
            "HSI": {"name": "Hang Seng Index", "region": "Hong Kong", "type": "market_cap_weighted"},
            "AXJO": {"name": "S&P/ASX 200", "region": "Australia", "type": "market_cap_weighted"},
            "STOXX50E": {"name": "EURO STOXX 50", "region": "Europe", "type": "market_cap_weighted"},
            "VIX": {"name": "CBOE Volatility Index", "region": "US", "type": "volatility"}
        }
    
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
            
            # Add region and type from metadata
            df['region'] = df['symbol'].map(lambda x: self.index_metadata.get(x, {}).get('region', 'Unknown'))
            df['index_type'] = df['symbol'].map(lambda x: self.index_metadata.get(x, {}).get('type', 'Unknown'))
            
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
        
        # Add index-specific calculations
        if not df.empty and 'close' in df.columns:
            # Calculate index returns
            df['daily_return'] = df['close'].pct_change() * 100  # in percentage
            
            # Calculate moving averages
            if len(df) >= 20:
                df['sma_20'] = df['close'].rolling(window=20).mean()
                df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
            
            if len(df) >= 50:
                df['sma_50'] = df['close'].rolling(window=50).mean()
                df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
            
            if len(df) >= 200:
                df['sma_200'] = df['close'].rolling(window=200).mean()
            
            # Calculate volatility (annualized)
            if len(df) >= 20:
                df['volatility_20d'] = df['daily_return'].rolling(window=20).std() * np.sqrt(252)
            
            # Calculate drawdown
            df['cumulative_max'] = df['close'].cummax()
            df['drawdown'] = (df['close'] - df['cumulative_max']) / df['cumulative_max'] * 100
        
        return df
    
    def extract_indices_by_region(
        self,
        region: str,
        **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        Extract indices by region
        
        Args:
            region: Region (US, Europe, Asia, etc.)
            **kwargs: Parameters for extract_index_time_series
            
        Returns:
            Dictionary mapping index symbol to DataFrame
        """
        logger.info(
            f"Extracting indices for region: {region}",
            source=self.source_name,
            region=region
        )
        
        # Filter indices by region
        region_indices = [
            symbol for symbol, meta in self.index_metadata.items()
            if meta.get('region', '').lower() == region.lower()
        ]
        
        if not region_indices:
            logger.warning(
                f"No indices found for region: {region}. Using default indices.",
                source=self.source_name,
                region=region
            )
            region_indices = self.default_indices
        
        return self.extract_time_series_batch(
            symbols=region_indices,
            **kwargs
        )
    
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
    
    def extract_index_quote(self, symbol: str) -> pd.DataFrame:
        """
        Extract real-time index quote
        
        Args:
            symbol: Index symbol
            
        Returns:
            DataFrame with quote data
        """
        logger.info(
            f"Extracting index quote for {symbol}",
            source=self.source_name,
            symbol=symbol
        )
        
        try:
            params = {
                "symbol": symbol
            }
            
            data = self._make_twelve_data_request(
                endpoint=self.endpoints["quote"],
                params=params
            )
            
            df = self._parse_quote_response(data)
            df['asset_type'] = 'index'
            df['extracted_at'] = datetime.now()
            
            # Add index metadata
            meta = self.index_metadata.get(symbol, {})
            df['index_name'] = meta.get('name', '')
            df['region'] = meta.get('region', '')
            df['index_type'] = meta.get('type', '')
            
            logger.info(
                f"Extracted index quote for {symbol}",
                source=self.source_name,
                symbol=symbol,
                price=df['close'].iloc[0] if not df.empty else None,
                name=df['index_name'].iloc[0] if not df.empty else None
            )
            
            return df
            
        except Exception as e:
            logger.error(
                f"Failed to extract index quote for {symbol}",
                exc_info=e,
                source=self.source_name,
                symbol=symbol
            )
            raise
    
    def extract_market_summary(self) -> pd.DataFrame:
        """
        Extract summary for all major indices
        
        Returns:
            DataFrame with market summary
        """
        logger.info(
            "Extracting market summary from major indices",
            source=self.source_name
        )
        
        summary_data = []
        
        for symbol in self.default_indices:
            try:
                quote_df = self.extract_index_quote(symbol)
                
                if not quote_df.empty:
                    summary_row = {
                        'symbol': symbol,
                        'name': self.index_metadata.get(symbol, {}).get('name', symbol),
                        'region': self.index_metadata.get(symbol, {}).get('region', 'Unknown'),
                        'last_price': quote_df['close'].iloc[0],
                        'change': quote_df['change'].iloc[0] if 'change' in quote_df.columns else None,
                        'percent_change': quote_df['percent_change'].iloc[0] if 'percent_change' in quote_df.columns else None,
                        'timestamp': quote_df['timestamp'].iloc[0] if 'timestamp' in quote_df.columns else datetime.now(),
                        'extracted_at': datetime.now()
                    }
                    summary_data.append(summary_row)
                
                # Add delay to avoid rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(
                    f"Failed to get summary for {symbol}",
                    exc_info=e,
                    source=self.source_name,
                    symbol=symbol
                )
                continue
        
        summary_df = pd.DataFrame(summary_data)
        
        if not summary_df.empty:
            # Calculate market sentiment
            up_markets = (summary_df['percent_change'] > 0).sum()
            total_markets = len(summary_df)
            summary_df['market_sentiment'] = 'Positive' if up_markets > total_markets / 2 else 'Negative'
        
        logger.info(
            f"Extracted market summary for {len(summary_df)} indices",
            source=self.source_name,
            index_count=len(summary_df)
        )
        
        return summary_df


# Update the factory.py imports
# Add this import to factory.py:
# from .etfs_indices import TwelveDataETFExtractor, TwelveDataIndexExtractor