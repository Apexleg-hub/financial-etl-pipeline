# src/transform/standardizer.py
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timezone
import pytz
from ..utils.logger import logger
from config.settings import settings



class DataStandardizer:
    """Standardize data across different sources and formats"""
    
    def __init__(self):
        self.config = settings.load_config("sources")
        self.default_timezone = pytz.timezone(
            self.config["extraction"].get("default_timezone", "UTC")
        )
        self.default_currency = self.config["transformation"].get("default_currency", "USD")
        
        # Define standard column names mapping
        self.column_standardization = {
            # Time columns
            "timestamp": ["timestamp", "time", "datetime", "date_time"],
            "date": ["date", "trade_date", "transaction_date"],
            "time": ["time", "trade_time"],
            
            # Price columns
            "open": ["open", "open_price", "opening_price"],
            "high": ["high", "high_price", "highest_price"],
            "low": ["low", "low_price", "lowest_price"],
            "close": ["close", "close_price", "closing_price", "price"],
            "volume": ["volume", "trade_volume", "vol"],
            
            # Identifier columns
            "symbol": ["symbol", "ticker", "instrument", "pair"],
            "exchange": ["exchange", "venue", "market"],
            
            # Financial columns
            "adj_close": ["adj_close", "adjusted_close", "adjusted_closing_price"],
            "dividend": ["dividend", "dividend_amount"],
            "split": ["split", "split_coefficient"],
            
            # Economic columns
            "value": ["value", "observation", "measure"],
            "series_id": ["series_id", "series_code", "indicator_id"],
            
            # Weather columns
            "temperature": ["temperature", "temp"],
            "humidity": ["humidity", "relative_humidity"],
            "pressure": ["pressure", "air_pressure"],
            "wind_speed": ["wind_speed", "wind_velocity"],
            
            # Sentiment columns
            "sentiment_score": ["sentiment_score", "sentiment", "polarity"],
            "confidence": ["confidence", "confidence_score"],
        }
        
        # Define standard data types
        self.standard_dtypes = {
            "timestamp": "datetime64[ns, UTC]",
            "date": "datetime64[ns]",
            "time": "object",
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "volume": "int64",
            "symbol": "object",
            "exchange": "object",
            "adj_close": "float64",
            "dividend": "float64",
            "split": "float64",
            "value": "float64",
            "series_id": "object",
            "temperature": "float64",
            "humidity": "float64",
            "pressure": "float64",
            "wind_speed": "float64",
            "sentiment_score": "float64",
            "confidence": "float64",
        }
        
        # Define time granularity mappings
        self.time_granularity_map = {
            "1m": "1T", "5m": "5T", "15m": "15T", "30m": "30T",
            "1h": "1H", "2h": "2H", "4h": "4H", "6h": "6H",
            "1d": "1D", "1w": "1W", "1M": "1M", "1q": "1Q", "1y": "1Y"
        }
        
        # Currency conversion rates (static, would be updated from API in production)
        self.currency_rates = {
            "USD": 1.0,
            "EUR": 0.85,
            "GBP": 0.73,
            "JPY": 110.0,
            "CAD": 1.25,
            "AUD": 1.35,
            "CHF": 0.92,
            "CNY": 6.45,
            "INR": 75.0
        }
        
        # Symbol/ticker standardization
        self.symbol_standardization = {
            # Stock symbols
            "AAPL.O": "AAPL", "AAPL.US": "AAPL",
            "MSFT.O": "MSFT", "MSFT.US": "MSFT",
            "GOOGL.O": "GOOGL", "GOOGL.US": "GOOGL",
            
            # Cryptocurrency symbols
            "BTC-USD": "BTCUSDT", "BTC/USD": "BTCUSDT",
            "ETH-USD": "ETHUSDT", "ETH/USD": "ETHUSDT",
            
            # Forex pairs
            "EUR/USD": "EURUSD", "GBP/USD": "GBPUSD",
            "USD/JPY": "USDJPY", "AUD/USD": "AUDUSD",
        }
    
    def standardize_dataframe(
        self,
        df: pd.DataFrame,
        data_type: str,
        source: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Standardize a DataFrame according to data type
        
        Args:
            df: Input DataFrame
            data_type: Type of data (stock, crypto, forex, economic, weather, sentiment)
            source: Data source name (optional)
        
        Returns:
            Standardized DataFrame
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for standardization")
            return df
        
        logger.info(
            f"Starting standardization for {data_type} data",
            data_type=data_type,
            source=source,
            original_shape=df.shape,
            columns=list(df.columns)
        )
        
        # Create a copy to avoid modifying original
        df_std = df.copy()
        
        # Step 1: Standardize column names
        df_std = self._standardize_column_names(df_std, data_type)
        
        # Step 2: Standardize data types
        df_std = self._standardize_data_types(df_std, data_type)
        
        # Step 3: Standardize time data
        df_std = self._standardize_time_data(df_std, data_type)
        
        # Step 4: Standardize symbols and identifiers
        df_std = self._standardize_symbols(df_std, data_type)
        
        # Step 5: Standardize currencies and units
        df_std = self._standardize_currencies_units(df_std, data_type)
        
        # Step 6: Standardize time granularity
        df_std = self._standardize_time_granularity(df_std, data_type)
        
        # Step 7: Ensure required columns
        df_std = self._ensure_required_columns(df_std, data_type)
        
        # Step 8: Sort and deduplicate
        df_std = self._finalize_dataframe(df_std, data_type)
        
        logger.info(
            f"Standardization completed for {data_type} data",
            data_type=data_type,
            original_shape=df.shape,
            standardized_shape=df_std.shape,
            standardized_columns=list(df_std.columns)
        )
        
        return df_std
    
    def _standardize_column_names(
        self,
        df: pd.DataFrame,
        data_type: str
    ) -> pd.DataFrame:
        """Standardize column names to common naming convention"""
        df_std = df.copy()
        column_mapping = {}
        
        for standardized_name, possible_names in self.column_standardization.items():
            # Check if any of the possible names exist in DataFrame
            for possible_name in possible_names:
                if possible_name in df_std.columns:
                    column_mapping[possible_name] = standardized_name
                    break
        
        # Apply renaming
        if column_mapping:
            df_std = df_std.rename(columns=column_mapping)
            logger.debug(
                f"Renamed columns: {column_mapping}",
                data_type=data_type,
                renamed_columns=column_mapping
            )
        
        # Convert column names to lowercase with underscores
        df_std.columns = [
            str(col).lower().replace(' ', '_').replace('-', '_')
            for col in df_std.columns
        ]
        
        # Remove special characters
        df_std.columns = [
            ''.join(c for c in col if c.isalnum() or c == '_')
            for col in df_std.columns
        ]
        
        return df_std
    
    def _standardize_data_types(
        self,
        df: pd.DataFrame,
        data_type: str
    ) -> pd.DataFrame:
        """Standardize data types according to schema"""
        df_std = df.copy()
        
        # Apply standard dtypes where applicable
        for col, dtype in self.standard_dtypes.items():
            if col in df_std.columns:
                try:
                    if 'datetime' in dtype:
                        # Handle datetime conversion
                        df_std[col] = pd.to_datetime(df_std[col], errors='coerce', utc=True)
                    elif dtype == 'float64':
                        df_std[col] = pd.to_numeric(df_std[col], errors='coerce')
                    elif dtype == 'int64':
                        # First convert to float, then to int (handles NaN)
                        df_std[col] = pd.to_numeric(df_std[col], errors='coerce')
                        df_std[col] = df_std[col].astype('Int64')  # Nullable integer
                    elif dtype == 'object':
                        df_std[col] = df_std[col].astype(str)
                except Exception as e:
                    logger.warning(
                        f"Failed to convert column {col} to {dtype}",
                        data_type=data_type,
                        column=col,
                        dtype=dtype,
                        error=str(e)
                    )
        
        return df_std
    
    def _standardize_time_data(
        self,
        df: pd.DataFrame,
        data_type: str
    ) -> pd.DataFrame:
        """Standardize time-related columns"""
        df_std = df.copy()
        
        # Ensure we have a datetime column
        time_columns = ['timestamp', 'date', 'datetime']
        existing_time_cols = [col for col in time_columns if col in df_std.columns]
        
        if not existing_time_cols:
            logger.warning(f"No time columns found in {data_type} data")
            return df_std
        
        # Use the first found time column as primary
        primary_time_col = existing_time_cols[0]
        
        # Convert to datetime with UTC timezone
        try:
            df_std['timestamp'] = pd.to_datetime(df_std[primary_time_col], utc=True)
            
            # If conversion failed for some rows, try different methods
            if df_std['timestamp'].isnull().any():
                # Try parsing as Unix timestamp
                mask = df_std['timestamp'].isnull()
                if mask.any():
                    try:
                        # Assume milliseconds if large numbers
                        df_std.loc[mask, 'timestamp'] = pd.to_datetime(
                            pd.to_numeric(df_std.loc[mask, primary_time_col]),
                            unit='ms',
                            utc=True
                        )
                    except:
                        pass
            
            # Ensure all timestamps are timezone-aware in UTC
            if df_std['timestamp'].dt.tz is None:
                df_std['timestamp'] = df_std['timestamp'].dt.tz_localize('UTC')
            else:
                df_std['timestamp'] = df_std['timestamp'].dt.tz_convert('UTC')
            
            # Extract date components
            df_std['date'] = df_std['timestamp'].dt.date
            df_std['year'] = df_std['timestamp'].dt.year
            df_std['month'] = df_std['timestamp'].dt.month
            df_std['day'] = df_std['timestamp'].dt.day
            df_std['hour'] = df_std['timestamp'].dt.hour
            df_std['minute'] = df_std['timestamp'].dt.minute
            df_std['day_of_week'] = df_std['timestamp'].dt.dayofweek
            df_std['day_of_year'] = df_std['timestamp'].dt.dayofyear
            df_std['week_of_year'] = df_std['timestamp'].dt.isocalendar().week
            
            # Remove the original time column if it's different
            if primary_time_col != 'timestamp':
                df_std = df_std.drop(columns=[primary_time_col])
            
        except Exception as e:
            logger.error(
                f"Failed to standardize time data",
                data_type=data_type,
                column=primary_time_col,
                error=str(e)
            )
        
        return df_std
    
    def _standardize_symbols(
        self,
        df: pd.DataFrame,
        data_type: str
    ) -> pd.DataFrame:
        """Standardize symbols and identifiers"""
        if 'symbol' not in df.columns:
            return df
        
        df_std = df.copy()
        
        # Clean symbol strings
        df_std['symbol'] = df_std['symbol'].astype(str).str.upper().str.strip()
        
        # Remove exchange suffixes/prefixes
        exchange_suffixes = ['.US', '.O', '.N', '.TO', '.V', '.AX', '.L', '.DE']
        for suffix in exchange_suffixes:
            df_std['symbol'] = df_std['symbol'].str.replace(suffix, '', regex=False)
        
        # Apply symbol standardization mapping
        def map_symbol(symbol):
            return self.symbol_standardization.get(symbol, symbol)
        
        df_std['symbol'] = df_std['symbol'].apply(map_symbol)
        
        # For cryptocurrencies, ensure standard format
        if data_type == 'crypto':
            # Remove dashes and slashes
            df_std['symbol'] = df_std['symbol'].str.replace('-', '').str.replace('/', '')
            # Ensure ends with USDT for USD pairs
            df_std['symbol'] = df_std['symbol'].apply(
                lambda x: f"{x}USDT" if not x.endswith('USDT') and len(x) <= 6 else x
            )
        
        # For forex, ensure no slashes
        elif data_type == 'forex':
            df_std['symbol'] = df_std['symbol'].str.replace('/', '')
        
        return df_std
    
    def _standardize_currencies_units(
        self,
        df: pd.DataFrame,
        data_type: str
    ) -> pd.DataFrame:
        """Standardize currencies and units to default"""
        df_std = df.copy()
        
        # Check for currency column
        if 'currency' in df_std.columns:
            # Convert all currencies to default
            def convert_to_usd(value, currency):
                if pd.isna(value) or pd.isna(currency):
                    return value
                rate = self.currency_rates.get(str(currency).upper(), 1.0)
                return value / rate if rate != 1.0 else value
            
            # Apply to all price columns
            price_cols = ['open', 'high', 'low', 'close', 'adj_close', 'value']
            for col in price_cols:
                if col in df_std.columns:
                    df_std[col] = df_std.apply(
                        lambda row: convert_to_usd(row[col], row.get('currency')),
                        axis=1
                    )
            
            # Mark that we've converted to USD
            df_std['currency'] = self.default_currency
        
        # Standardize units for specific data types
        if data_type == 'weather':
            # Convert temperature to Celsius if needed
            if 'temperature' in df_std.columns and 'units' in df_std.columns:
                mask = df_std['units'] == 'imperial'
                df_std.loc[mask, 'temperature'] = (df_std.loc[mask, 'temperature'] - 32) * 5/9
                df_std['units'] = 'metric'
            
            # Convert wind speed to m/s if in mph
            if 'wind_speed' in df_std.columns and 'units' in df_std.columns:
                mask = df_std['units'] == 'imperial'
                df_std.loc[mask, 'wind_speed'] = df_std.loc[mask, 'wind_speed'] * 0.44704
        
        elif data_type == 'economic':
            # Standardize units for economic indicators
            if 'units' in df_std.columns:
                # Remove whitespace and standardize
                df_std['units'] = df_std['units'].str.strip().str.lower()
                
                # Common unit mappings
                unit_mappings = {
                    'percent': 'percentage',
                    'pct': 'percentage',
                    '%': 'percentage',
                    'us dollars': 'usd',
                    'dollars': 'usd',
                    '$': 'usd',
                    'euros': 'eur',
                    'pounds': 'gbp',
                    'yens': 'jpy'
                }
                
                df_std['units'] = df_std['units'].replace(unit_mappings)
        
        return df_std
    
    def _standardize_time_granularity(
        self,
        df: pd.DataFrame,
        data_type: str
    ) -> pd.DataFrame:
        """Standardize time granularity for time-series data"""
        if 'timestamp' not in df.columns:
            return df
        
        df_std = df.copy()
        
        # Detect current granularity
        if len(df_std) > 1:
            time_diffs = df_std['timestamp'].diff().dropna()
            if not time_diffs.empty:
                median_diff = time_diffs.median()
                
                # Map to standard granularity
                if median_diff <= pd.Timedelta(minutes=1):
                    granularity = '1m'
                elif median_diff <= pd.Timedelta(minutes=5):
                    granularity = '5m'
                elif median_diff <= pd.Timedelta(minutes=15):
                    granularity = '15m'
                elif median_diff <= pd.Timedelta(minutes=30):
                    granularity = '30m'
                elif median_diff <= pd.Timedelta(hours=1):
                    granularity = '1h'
                elif median_diff <= pd.Timedelta(days=1):
                    granularity = '1d'
                elif median_diff <= pd.Timedelta(days=7):
                    granularity = '1w'
                elif median_diff <= pd.Timedelta(days=30):
                    granularity = '1M'
                else:
                    granularity = 'custom'
                
                df_std['granularity'] = granularity
        
        # Resample to target granularity if needed
        target_granularity = self._get_target_granularity(data_type)
        
        if target_granularity and 'granularity' in df_std.columns:
            if df_std['granularity'].iloc[0] != target_granularity:
                df_std = self._resample_to_granularity(df_std, target_granularity, data_type)
        
        return df_std
    
    def _get_target_granularity(self, data_type: str) -> Optional[str]:
        """Get target granularity for data type"""
        granularity_map = {
            'stock': '1d',
            'crypto': '1h',
            'forex': '1h',
            'economic': '1d',
            'weather': '1h',
            'sentiment': '1d'
        }
        return granularity_map.get(data_type)
    
    def _resample_to_granularity(
        self,
        df: pd.DataFrame,
        target_granularity: str,
        data_type: str
    ) -> pd.DataFrame:
        """Resample data to target granularity"""
        if 'timestamp' not in df.columns:
            return df
        
        # Set timestamp as index
        df_resampled = df.set_index('timestamp')
        
        # Define aggregation rules based on data type
        if data_type in ['stock', 'crypto', 'forex']:
            agg_rules = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'symbol': 'first',
                'exchange': 'first'
            }
        elif data_type == 'economic':
            agg_rules = {
                'value': 'last',  # Most recent value
                'series_id': 'first'
            }
        elif data_type == 'weather':
            agg_rules = {
                'temperature': 'mean',
                'humidity': 'mean',
                'pressure': 'mean',
                'wind_speed': 'mean',
                'location': 'first'
            }
        elif data_type == 'sentiment':
            agg_rules = {
                'sentiment_score': 'mean',
                'confidence': 'mean',
                'entity': 'first'
            }
        else:
            return df
        
        # Filter to only columns that exist
        agg_rules = {k: v for k, v in agg_rules.items() if k in df_resampled.columns}
        
        # Resample
        try:
            resampled = df_resampled.resample(self.time_granularity_map[target_granularity]).agg(agg_rules)
            
            # Reset index
            resampled = resampled.reset_index()
            
            # Update granularity
            resampled['granularity'] = target_granularity
            
            logger.info(
                f"Resampled data from {df_resampled.index.freq} to {target_granularity}",
                original_rows=len(df),
                resampled_rows=len(resampled),
                target_granularity=target_granularity
            )
            
            return resampled
            
        except Exception as e:
            logger.error(
                f"Failed to resample data to {target_granularity}",
                data_type=data_type,
                target_granularity=target_granularity,
                error=str(e)
            )
            return df
    
    def _ensure_required_columns(
        self,
        df: pd.DataFrame,
        data_type: str
    ) -> pd.DataFrame:
        """Ensure required columns exist for each data type"""
        required_columns = {
            'stock': ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'],
            'crypto': ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'exchange'],
            'forex': ['timestamp', 'symbol', 'open', 'high', 'low', 'close'],
            'economic': ['timestamp', 'series_id', 'value'],
            'weather': ['timestamp', 'location', 'temperature', 'humidity', 'pressure', 'wind_speed'],
            'sentiment': ['timestamp', 'entity', 'sentiment_score', 'confidence']
        }
        
        required = required_columns.get(data_type, [])
        
        # Add missing required columns with NaN values
        for col in required:
            if col not in df.columns:
                df[col] = np.nan
                logger.warning(
                    f"Added missing required column: {col}",
                    data_type=data_type,
                    missing_column=col
                )
        
        return df
    
    def _finalize_dataframe(
        self,
        df: pd.DataFrame,
        data_type: str
    ) -> pd.DataFrame:
        """Final cleanup and organization"""
        if df.empty:
            return df
        
        df_final = df.copy()
        
        # 1. Sort by timestamp (if exists)
        if 'timestamp' in df_final.columns:
            df_final = df_final.sort_values('timestamp').reset_index(drop=True)
        
        # 2. Remove duplicates
        before_dedup = len(df_final)
        
        # Define duplicate detection columns
        duplicate_cols = ['timestamp']
        if 'symbol' in df_final.columns:
            duplicate_cols.append('symbol')
        if 'series_id' in df_final.columns:
            duplicate_cols.append('series_id')
        if 'location' in df_final.columns:
            duplicate_cols.append('location')
        
        duplicate_cols = [col for col in duplicate_cols if col in df_final.columns]
        
        if duplicate_cols:
            df_final = df_final.drop_duplicates(subset=duplicate_cols, keep='last')
            duplicates_removed = before_dedup - len(df_final)
            if duplicates_removed > 0:
                logger.info(
                    f"Removed {duplicates_removed} duplicate records",
                    data_type=data_type,
                    duplicates_removed=duplicates_removed
                )
        
        # 3. Add metadata columns
        df_final['standardized_at'] = datetime.utcnow()
        df_final['data_type'] = data_type
        
        # 4. Reorder columns (timestamp first, metadata last)
        timestamp_cols = [col for col in df_final.columns if 'time' in col or 'date' in col]
        metadata_cols = ['standardized_at', 'data_type', 'granularity', 'units']
        other_cols = [
            col for col in df_final.columns 
            if col not in timestamp_cols + metadata_cols
        ]
        
        ordered_cols = timestamp_cols + other_cols + [
            col for col in metadata_cols if col in df_final.columns
        ]
        
        df_final = df_final[ordered_cols]
        
        return df_final
    
    def standardize_multiple_dataframes(
        self,
        dataframes: Dict[str, pd.DataFrame],
        data_types: Dict[str, str]
    ) -> Dict[str, pd.DataFrame]:
        """
        Standardize multiple DataFrames at once
        
        Args:
            dataframes: Dictionary of {name: DataFrame}
            data_types: Dictionary of {name: data_type}
        
        Returns:
            Dictionary of standardized DataFrames
        """
        standardized = {}
        
        for name, df in dataframes.items():
            data_type = data_types.get(name, 'unknown')
            
            try:
                standardized_df = self.standardize_dataframe(df, data_type, source=name)
                standardized[name] = standardized_df
                
                logger.info(
                    f"Standardized {name} ({data_type})",
                    name=name,
                    data_type=data_type,
                    original_shape=df.shape,
                    standardized_shape=standardized_df.shape
                )
                
            except Exception as e:
                logger.error(
                    f"Failed to standardize {name}",
                    exc_info=e,
                    name=name,
                    data_type=data_type
                )
                standardized[name] = pd.DataFrame()  # Return empty DataFrame on failure
        
        return standardized