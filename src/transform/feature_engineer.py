# src/transform/feature_engineer.py
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from ..utils.logger import logger


class FeatureEngineer:
    """Feature engineering for time series data"""
    
    def __init__(self):
        self.default_lags = [1, 2, 3, 5, 7, 14, 21, 30]
    
    def create_time_series_features(
        self,
        df: pd.DataFrame,
        value_column: str,
        date_column: str = 'date',
        group_column: Optional[str] = None,
        lags: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """
        Create time series features including lags, moving averages, etc.
        
        Args:
            df: Input DataFrame
            value_column: Column to create features from
            date_column: Date column name
            group_column: Column to group by (e.g., symbol)
            lags: List of lag periods
        
        Returns:
            DataFrame with engineered features
        """
        if lags is None:
            lags = self.default_lags
        
        logger.info(
            "Creating time series features",
            value_column=value_column,
            lags=lags,
            group_column=group_column
        )
        
        # Sort by date
        df = df.sort_values(date_column).copy()
        
        # Create features
        if group_column:
            # Group by symbol or other grouping column
            groups = df.groupby(group_column)
            feature_dfs = []
            
            for name, group in groups:
                group_features = self._create_group_features(
                    group, value_column, date_column, lags
                )
                feature_dfs.append(group_features)
            
            result = pd.concat(feature_dfs, ignore_index=True)
        else:
            result = self._create_group_features(df, value_column, date_column, lags)
        
        logger.info(
            "Time series features created",
            original_columns=list(df.columns),
            new_columns=list(result.columns),
            rows_added=len(result) - len(df)
        )
        
        return result
    
    def _create_group_features(
        self,
        df: pd.DataFrame,
        value_column: str,
        date_column: str,
        lags: List[int]
    ) -> pd.DataFrame:
        """Create features for a single group"""
        result = df.copy()
        
        # Lag features
        for lag in lags:
            result[f'{value_column}_lag_{lag}'] = result[value_column].shift(lag)
        
        # Rolling statistics
        windows = [3, 7, 14, 30]
        for window in windows:
            result[f'{value_column}_ma_{window}'] = (
                result[value_column].rolling(window=window, min_periods=1).mean()
            )
            result[f'{value_column}_std_{window}'] = (
                result[value_column].rolling(window=window, min_periods=2).std()
            )
        
        # Percent changes
        result[f'{value_column}_pct_change_1'] = result[value_column].pct_change(1)
        result[f'{value_column}_pct_change_7'] = result[value_column].pct_change(7)
        
        # Volatility features
        result[f'{value_column}_range'] = (
            result.get('high', result[value_column]) - 
            result.get('low', result[value_column])
        )
        
        # Day of week, month, quarter features
        if date_column in result.columns:
            result['day_of_week'] = result[date_column].dt.dayofweek
            result['day_of_month'] = result[date_column].dt.day
            result['month'] = result[date_column].dt.month
            result['quarter'] = result[date_column].dt.quarter
            result['year'] = result[date_column].dt.year
            result['is_month_end'] = result[date_column].dt.is_month_end
            result['is_quarter_end'] = result[date_column].dt.is_quarter_end
        
        return result
    
    def create_sentiment_features(
        self,
        df: pd.DataFrame,
        text_column: str,
        date_column: str = 'timestamp'
    ) -> pd.DataFrame:
        """
        Create sentiment features from text data
        
        Args:
            df: Input DataFrame with text
            text_column: Column containing text
            date_column: Date column for aggregation
        
        Returns:
            DataFrame with sentiment features
        """
        try:
            from textblob import TextBlob
        except ImportError:
            logger.warning("TextBlob not installed. Skipping sentiment analysis.")
            return df
        
        logger.info("Creating sentiment features", text_column=text_column)
        
        result = df.copy()
        
        # Calculate sentiment polarity and subjectivity
        sentiments = result[text_column].apply(
            lambda x: TextBlob(str(x)).sentiment if pd.notnull(x) else (0, 0)
        )
        
        result['sentiment_polarity'] = sentiments.apply(lambda x: x[0])
        result['sentiment_subjectivity'] = sentiments.apply(lambda x: x[1])
        
        # Categorize sentiment
        result['sentiment_label'] = pd.cut(
            result['sentiment_polarity'],
            bins=[-1, -0.1, 0.1, 1],
            labels=['negative', 'neutral', 'positive']
        )
        
        # Aggregate sentiment by date (if multiple texts per date)
        if date_column in result.columns:
            agg_features = result.groupby(date_column).agg({
                'sentiment_polarity': ['mean', 'std', 'count'],
                'sentiment_subjectivity': 'mean'
            }).reset_index()
            
            # Flatten column names
            agg_features.columns = [
                f"{col[0]}_{col[1]}" if col[1] else col[0]
                for col in agg_features.columns
            ]
            
            result = result.merge(agg_features, on=date_column, how='left')
        
        logger.info(
            "Sentiment features created",
            sentiment_stats={
                'mean_polarity': result['sentiment_polarity'].mean(),
                'positive_count': (result['sentiment_label'] == 'positive').sum(),
                'negative_count': (result['sentiment_label'] == 'negative').sum()
            }
        )
        
        return result