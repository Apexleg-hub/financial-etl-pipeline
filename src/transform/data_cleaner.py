# src/transform/data_cleaner.py
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from ..utils.logger import logger
from config.settings import settings


class DataCleaner:
    """Data cleaning and validation"""
    
    def __init__(self):
        self.config = settings.load_config("sources")
        self.missing_threshold = self.config["transformation"]["missing_value_threshold"]
    
    def clean_dataframe(
        self,
        df: pd.DataFrame,
        schema: Dict[str, str],
        source: str
    ) -> pd.DataFrame:
        """
        Clean DataFrame according to schema
        
        Args:
            df: Input DataFrame
            schema: Expected column types
            source: Data source name
        
        Returns:
            Cleaned DataFrame
        """
        logger.info(f"Starting data cleaning for {source}", source=source)
        
        # Create a copy to avoid modifying original
        df_clean = df.copy()
        
        # 1. Handle duplicates
        df_clean = self._remove_duplicates(df_clean, source)
        
        # 2. Enforce schema
        df_clean = self._enforce_schema(df_clean, schema, source)
        
        # 3. Handle missing values
        df_clean = self._handle_missing_values(df_clean, source)
        
        # 4. Standardize timestamps
        df_clean = self._standardize_timestamps(df_clean, source)
        
        # 5. Validate data
        is_valid, errors = self._validate_data(df_clean, schema, source)
        
        if not is_valid:
            logger.error(
                f"Data validation failed for {source}",
                source=source,
                errors=errors
            )
            raise ValueError(f"Data validation failed: {errors}")
        
        logger.info(
            f"Data cleaning completed for {source}",
            source=source,
            original_rows=len(df),
            cleaned_rows=len(df_clean),
            columns=list(df_clean.columns)
        )
        
        return df_clean
    
    def _remove_duplicates(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """Remove duplicate rows"""
        initial_count = len(df)
        
        # Identify duplicate columns (excluding timestamp columns)
        dup_cols = [col for col in df.columns if col not in ['timestamp', 'date', 'created_at']]
        
        if dup_cols:
            df_deduped = df.drop_duplicates(subset=dup_cols, keep='first')
            duplicates_removed = initial_count - len(df_deduped)
            
            if duplicates_removed > 0:
                logger.warning(
                    f"Removed {duplicates_removed} duplicates from {source}",
                    source=source,
                    duplicates_removed=duplicates_removed
                )
            
            return df_deduped
        
        return df
    
    def _enforce_schema(
        self,
        df: pd.DataFrame,
        schema: Dict[str, str],
        source: str
    ) -> pd.DataFrame:
        """Enforce column types according to schema"""
        for column, dtype in schema.items():
            if column in df.columns:
                try:
                    if dtype == 'datetime':
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                    elif dtype == 'float':
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    elif dtype == 'int':
                        df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                    elif dtype == 'str':
                        df[column] = df[column].astype(str)
                except Exception as e:
                    logger.warning(
                        f"Failed to convert column {column} to {dtype}",
                        source=source,
                        column=column,
                        dtype=dtype,
                        error=str(e)
                    )
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """Handle missing values based on column type"""
        missing_percentage = (df.isnull().sum() / len(df)).to_dict()
        
        for column in df.columns:
            missing_pct = missing_percentage.get(column, 0)
            
            if missing_pct > self.missing_threshold:
                logger.warning(
                    f"High missing values in column {column}",
                    source=source,
                    column=column,
                    missing_percentage=missing_pct,
                    threshold=self.missing_threshold
                )
            
            # Handle based on column type
            if df[column].dtype == 'object':
                # For categorical/text, fill with mode
                if missing_pct > 0 and missing_pct <= self.missing_threshold:
                    mode_value = df[column].mode()
                    if not mode_value.empty:
                        df[column] = df[column].fillna(mode_value.iloc[0])
            elif pd.api.types.is_numeric_dtype(df[column]):
                # For numeric, fill with median
                if missing_pct > 0 and missing_pct <= self.missing_threshold:
                    median_value = df[column].median()
                    df[column] = df[column].fillna(median_value)
            elif pd.api.types.is_datetime64_any_dtype(df[column]):
                # For datetime, forward fill
                df[column] = df[column].ffill()
        
        return df
    
    def _standardize_timestamps(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """Standardize timestamp columns to UTC"""
        time_columns = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
        
        for col in time_columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # Convert to UTC
                if df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_convert('UTC')
                else:
                    # Assume UTC if no timezone
                    df[col] = df[col].dt.tz_localize('UTC')
        
        return df
    
    def _validate_data(
        self,
        df: pd.DataFrame,
        schema: Dict[str, str],
        source: str
    ) -> Tuple[bool, List[str]]:
        """Validate data quality"""
        errors = []
        
        # Check required columns
        required_cols = list(schema.keys())
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")
        
        # Check for nulls in critical columns
        critical_cols = ['timestamp', 'date', 'value', 'price']
        for col in critical_cols:
            if col in df.columns and df[col].isnull().any():
                null_count = df[col].isnull().sum()
                errors.append(f"Null values in critical column {col}: {null_count}")
        
        # Check for negative values where not expected
        positive_cols = ['price', 'value', 'volume', 'open', 'high', 'low', 'close']
        for col in positive_cols:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    errors.append(f"Negative values in {col}: {negative_count}")
        
        # Check for anomalies using Z-score
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].std() > 0:  # Avoid division by zero
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                anomaly_threshold = self.config["transformation"]["anomaly_zscore_threshold"]
                anomalies = (z_scores > anomaly_threshold).sum()
                
                if anomalies > 0:
                    errors.append(f"Anomalies detected in {col}: {anomalies}")
        
        return len(errors) == 0, errors