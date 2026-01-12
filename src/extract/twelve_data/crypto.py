

# src/extract/twelve_data/crypto.py
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import pandas as pd

from .time_series import TwelveDataTimeSeriesExtractor
from src.utils.logger import logger


class TwelveDataCryptoExtractor(TwelveDataTimeSeriesExtractor):
    """
    Extractor for Cryptocurrency data from Twelve Data
    """
    
    def __init__(self):
        """
        Initialize Crypto extractor
        """
        super().__init__()
        
        # Crypto-specific configuration
        self.crypto_config = self.config.get("crypto", {})
        self.default_cryptos = self.crypto_config.get("default_cryptos", [
            "BTC/USD", "ETH/USD", "BNB/USD", "XRP/USD",
            "ADA/USD", "SOL/USD", "DOT/USD", "DOGE/USD"
        ])
        self.default_quote_currency = self.crypto_config.get("default_quote_currency", "USD")
    
    def get_cryptocurrencies(self) -> pd.DataFrame:
        """
        Get all available cryptocurrencies
        
        Returns:
            DataFrame with cryptocurrency information
        """
        logger.info(
            "Fetching available cryptocurrencies",
            source=self.source_name
        )
        
        try:
            data = self._make_twelve_data_request(
                endpoint=self.endpoints["cryptocurrencies"]
            )
            
            df = self._parse_symbol_list_response(data)
            
            # Add crypto-specific metadata
            df['asset_type'] = 'crypto'
            df['extracted_at'] = datetime.now()
            
            logger.info(
                f"Found {len(df)} cryptocurrencies",
                source=self.source_name,
                crypto_count=len(df)
            )
            
            return df
            
        except Exception as e:
            logger.error(
                "Failed to fetch cryptocurrencies",
                exc_info=e,
                source=self.source_name
            )
            raise
    
    def extract_crypto_time_series(
        self,
        symbol: str,
        quote_currency: str = "USD",
        interval: str = "1day",
        output_size: int = 1000,
        **kwargs
    ) -> pd.DataFrame:
        """
        Extract cryptocurrency time series data
        
        Args:
            symbol: Cryptocurrency symbol (e.g., 'BTC')
            quote_currency: Quote currency (e.g., 'USD', 'EUR')
            interval: Time interval
            output_size: Number of data points
            **kwargs: Additional parameters
            
        Returns:
            DataFrame with crypto time series
        """
        logger.info(
            f"Extracting crypto data for {symbol}/{quote_currency}",
            source=self.source_name,
            symbol=symbol,
            quote_currency=quote_currency,
            interval=interval
        )
        
        # Construct full symbol
        full_symbol = f"{symbol}/{quote_currency}"
        
        # Extract time series with crypto asset type
        df = self.extract_time_series(
            symbol=full_symbol,
            interval=interval,
            output_size=output_size,
            asset_type='crypto',
            **kwargs
        )
        
        # Add crypto-specific calculations
        if not df.empty:
            if 'close' in df.columns:
                df['log_returns'] = np.log(df['close'] / df['close'].shift(1))
                df['volatility_20d'] = df['log_returns'].rolling(window=20).std() * np.sqrt(365)
                
                # Calculate crypto-specific metrics
                if 'volume' in df.columns:
                    df['market_sentiment'] = df['close'].rolling(window=10).corr(df['volume'])
        
        return df
    
    def extract_major_cryptos(self, quote_currency: str = "USD", **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Extract data for major cryptocurrencies
        
        Args:
            quote_currency: Quote currency for all pairs
            **kwargs: Parameters for extract_crypto_time_series
            
        Returns:
            Dictionary mapping crypto symbol to DataFrame
        """
        logger.info(
            "Extracting major cryptocurrencies",
            source=self.source_name,
            cryptos=self.default_cryptos,
            quote_currency=quote_currency
        )
        
        # Update symbols with quote currency
        symbols = [
            symbol.replace('/USD', f'/{quote_currency}') 
            if '/USD' in symbol else f'{symbol}/{quote_currency}'
            for symbol in self.default_cryptos
        ]
        
        return self.extract_time_series_batch(
            symbols=symbols,
            **kwargs
        )
    
    def extract_crypto_quote(self, symbol: str, quote_currency: str = "USD") -> pd.DataFrame:
        """
        Extract real-time cryptocurrency quote
        
        Args:
            symbol: Cryptocurrency symbol
            quote_currency: Quote currency
            
        Returns:
            DataFrame with quote data
        """
        logger.info(
            f"Extracting crypto quote for {symbol}/{quote_currency}",
            source=self.source_name,
            symbol=symbol,
            quote_currency=quote_currency
        )
        
        try:
            full_symbol = f"{symbol}/{quote_currency}"
            
            params = {
                "symbol": full_symbol,
                "type": "digital_currency"
            }
            
            data = self._make_twelve_data_request(
                endpoint=self.endpoints["quote"],
                params=params
            )
            
            df = self._parse_quote_response(data)
            df['asset_type'] = 'crypto'
            df['base_currency'] = symbol
            df['quote_currency'] = quote_currency
            df['extracted_at'] = datetime.now()
            
            logger.info(
                f"Extracted crypto quote for {symbol}/{quote_currency}",
                source=self.source_name,
                symbol=symbol,
                price=df['close'].iloc[0] if not df.empty else None,
                change=df['percent_change'].iloc[0] if not df.empty else None
            )
            
            return df
            
        except Exception as e:
            logger.error(
                f"Failed to extract crypto quote for {symbol}",
                exc_info=e,
                source=self.source_name,
                symbol=symbol
            )
            raise