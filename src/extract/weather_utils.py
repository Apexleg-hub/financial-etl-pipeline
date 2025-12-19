# src/extract/weather_utils.py
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import numpy as np
from ..utils.logger import logger


class WeatherDataProcessor:
    """Utility class for processing weather data"""
    
    @staticmethod
    def aggregate_hourly_to_daily(hourly_df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate hourly weather data to daily
        
        Args:
            hourly_df: DataFrame with hourly weather data
        
        Returns:
            DataFrame with daily aggregated data
        """
        if hourly_df.empty:
            return pd.DataFrame()
        
        # Ensure timestamp is datetime
        hourly_df = hourly_df.copy()
        if 'timestamp' in hourly_df.columns:
            hourly_df['date'] = pd.to_datetime(hourly_df['timestamp']).dt.date
        elif 'datetime' in hourly_df.columns:
            hourly_df['date'] = pd.to_datetime(hourly_df['datetime']).dt.date
        else:
            logger.error("No timestamp column found in hourly data")
            return pd.DataFrame()
        
        # Group by date and location
        group_cols = ['date', 'location', 'latitude', 'longitude', 'units', 'source']
        group_cols = [col for col in group_cols if col in hourly_df.columns]
        
        # Define aggregation rules
        agg_rules = {}
        
        # For temperature-related columns, compute min, max, mean
        temp_cols = ['temperature', 'feels_like', 'temp_min', 'temp_max', 
                    'dew_point', 'temp_day', 'temp_night', 'temp_eve', 'temp_morn']
        for col in temp_cols:
            if col in hourly_df.columns:
                agg_rules[col] = ['min', 'max', 'mean']
        
        # For pressure, humidity, cloudiness - compute mean
        mean_cols = ['pressure', 'humidity', 'cloudiness', 'visibility', 'uvi']
        for col in mean_cols:
            if col in hourly_df.columns:
                agg_rules[col] = 'mean'
        
        # For wind - compute max and mean
        wind_cols = ['wind_speed', 'wind_gust']
        for col in wind_cols:
            if col in hourly_df.columns:
                agg_rules[col] = ['max', 'mean']
        
        # For precipitation - compute sum
        precip_cols = ['rain_1h', 'rain_3h', 'snow_1h', 'snow_3h', 'pop']
        for col in precip_cols:
            if col in hourly_df.columns:
                agg_rules[col] = 'sum'
        
        # For categorical data - take the most frequent value
        cat_cols = ['weather_main', 'weather_description', 'weather_icon']
        for col in cat_cols:
            if col in hourly_df.columns:
                agg_rules[col] = lambda x: x.mode()[0] if not x.mode().empty else None
        
        if not agg_rules:
            logger.warning("No columns to aggregate")
            return pd.DataFrame()
        
        # Perform aggregation
        aggregated = hourly_df.groupby(group_cols).agg(agg_rules).reset_index()
        
        # Flatten multi-level column names
        aggregated.columns = ['_'.join(col).strip('_') for col in aggregated.columns.values]
        
        # Rename columns for clarity
        column_rename = {}
        for col in aggregated.columns:
            if col.endswith('_min'):
                column_rename[col] = col.replace('_min', '_daily_min')
            elif col.endswith('_max'):
                column_rename[col] = col.replace('_max', '_daily_max')
            elif col.endswith('_mean'):
                column_rename[col] = col.replace('_mean', '_daily_avg')
            elif col.endswith('_sum'):
                column_rename[col] = col.replace('_sum', '_daily_total')
        
        aggregated = aggregated.rename(columns=column_rename)
        
        logger.info(
            f"Aggregated {len(hourly_df)} hourly records to {len(aggregated)} daily records",
            hourly_records=len(hourly_df),
            daily_records=len(aggregated)
        )
        
        return aggregated
    
    @staticmethod
    def calculate_weather_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate additional weather features
        
        Args:
            df: DataFrame with weather data
        
        Returns:
            DataFrame with additional features
        """
        if df.empty:
            return df
        
        df = df.copy()
        
        # Calculate temperature range
        if 'temperature_daily_max' in df.columns and 'temperature_daily_min' in df.columns:
            df['temperature_daily_range'] = df['temperature_daily_max'] - df['temperature_daily_min']
        elif 'temp_max' in df.columns and 'temp_min' in df.columns:
            df['temperature_range'] = df['temp_max'] - df['temp_min']
        
        # Calculate heat index (simplified)
        if 'temperature_daily_avg' in df.columns and 'humidity_daily_avg' in df.columns:
            df['heat_index'] = df.apply(
                lambda row: WeatherDataProcessor._calculate_heat_index(
                    row['temperature_daily_avg'],
                    row['humidity_daily_avg']
                ), axis=1
            )
        
        # Calculate wind chill
        if 'temperature_daily_avg' in df.columns and 'wind_speed_daily_avg' in df.columns:
            df['wind_chill'] = df.apply(
                lambda row: WeatherDataProcessor._calculate_wind_chill(
                    row['temperature_daily_avg'],
                    row['wind_speed_daily_avg']
                ), axis=1
            )
        
        # Calculate comfort index (simplified)
        if 'temperature_daily_avg' in df.columns and 'humidity_daily_avg' in df.columns:
            df['comfort_index'] = df.apply(
                lambda row: WeatherDataProcessor._calculate_comfort_index(
                    row['temperature_daily_avg'],
                    row['humidity_daily_avg']
                ), axis=1
            )
        
        # Calculate weather severity score
        severity_factors = []
        if 'wind_speed_daily_max' in df.columns:
            severity_factors.append(df['wind_speed_daily_max'] / 20)  # Normalize
        if 'precipitation_daily_total' in df.columns:
            severity_factors.append(df['precipitation_daily_total'] / 50)  # Normalize
        if 'temperature_daily_range' in df.columns:
            severity_factors.append(df['temperature_daily_range'] / 15)  # Normalize
        
        if severity_factors:
            df['weather_severity_score'] = sum(severity_factors) / len(severity_factors)
        
        # Add season based on date
        if 'date' in df.columns:
            df['season'] = pd.to_datetime(df['date']).dt.month.apply(
                lambda m: 'Winter' if m in [12, 1, 2] else
                         'Spring' if m in [3, 4, 5] else
                         'Summer' if m in [6, 7, 8] else 'Fall'
            )
        
        # Add weekday/weekend flag
        if 'date' in df.columns:
            df['is_weekend'] = pd.to_datetime(df['date']).dt.dayofweek >= 5
        
        logger.info(f"Calculated weather features for {len(df)} records")
        
        return df
    
    @staticmethod
    def _calculate_heat_index(temperature: float, humidity: float) -> float:
        """Calculate heat index (simplified formula)"""
        if temperature < 26.7:  # Below 80°F
            return temperature
        
        # Simplified heat index calculation
        hi = -42.379 + 2.04901523 * temperature + 10.14333127 * humidity
        hi += -0.22475541 * temperature * humidity - 6.83783e-3 * temperature**2
        hi += -5.481717e-2 * humidity**2 + 1.22874e-3 * temperature**2 * humidity
        hi += 8.5282e-4 * temperature * humidity**2 - 1.99e-6 * temperature**2 * humidity**2
        
        return hi
    
    @staticmethod
    def _calculate_wind_chill(temperature: float, wind_speed: float) -> float:
        """Calculate wind chill (simplified formula)"""
        if temperature > 10:  # Above 50°F
            return temperature
        
        # Simplified wind chill calculation
        wc = 13.12 + 0.6215 * temperature - 11.37 * (wind_speed ** 0.16)
        wc += 0.3965 * temperature * (wind_speed ** 0.16)
        
        return wc
    
    @staticmethod
    def _calculate_comfort_index(temperature: float, humidity: float) -> float:
        """Calculate comfort index (0-100, higher is more comfortable)"""
        # Ideal conditions: 21°C (70°F) and 50% humidity
        temp_diff = abs(temperature - 21)
        hum_diff = abs(humidity - 50)
        
        # Normalize differences
        temp_score = max(0, 100 - temp_diff * 5)
        hum_score = max(0, 100 - hum_diff * 2)
        
        # Combined score
        comfort_score = (temp_score * 0.6 + hum_score * 0.4)
        
        return comfort_score
    
    @staticmethod
    def clean_weather_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean weather data by handling outliers and missing values
        
        Args:
            df: DataFrame with weather data
        
        Returns:
            Cleaned DataFrame
        """
        if df.empty:
            return df
        
        df = df.copy()
        
        # Handle missing values
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if df[col].isnull().any():
                # For temperature, use forward fill then backward fill
                if 'temp' in col.lower() or 'temperature' in col.lower():
                    df[col] = df[col].ffill().bfill()
                # For precipitation, fill with 0
                elif 'rain' in col.lower() or 'snow' in col.lower() or 'precip' in col.lower():
                    df[col] = df[col].fillna(0)
                # For other numeric columns, use median
                else:
                    median_val = df[col].median()
                    df[col] = df[col].fillna(median_val)
        
        # Handle outliers using IQR method
        for col in numeric_cols:
            if df[col].notna().all():
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Cap outliers
                df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
        
        # Validate data ranges
        if 'temperature' in df.columns:
            df['temperature'] = df['temperature'].clip(-50, 60)  # Reasonable Earth temperatures
        if 'humidity' in df.columns:
            df['humidity'] = df['humidity'].clip(0, 100)
        if 'wind_speed' in df.columns:
            df['wind_speed'] = df['wind_speed'].clip(0, 150)  # 150 m/s is extreme
        
        logger.info(f"Cleaned weather data for {len(df)} records")
        
        return df