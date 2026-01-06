# src/extract/fred.py
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import requests
from .base_extractor import BaseExtractor
from config.settings import settings
from ..utils.logger import logger
from ..utils.rate_limiter import RateLimitConfig, rate_limiter


class FREDExtractor(BaseExtractor):
    """FRED (Federal Reserve Economic Data) API extractor"""
    
    def __init__(self):
        super().__init__("fred")
        source_config = self.config["sources"]["fred"]
        self._base_url = source_config["base_url"]
        self.endpoints = source_config["endpoints"]
        
        # Register rate limit
        rate_config = RateLimitConfig(
            max_requests=source_config["rate_limit"],
            time_window=60,
            retry_delay=60
        )
        rate_limiter.register_source(self.source_name, rate_config)
    
    @property
    def api_key(self) -> str:
        return settings.fred_api_key
    
    @property
    def base_url(self) -> str:
        return self._base_url
    
    def extract_series(
        self,
        series_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        frequency: Optional[str] = None,
        aggregation_method: str = 'avg'
    ) -> pd.DataFrame:
        """
        Extract economic time series data
        
        Args:
            series_id: FRED series ID (e.g., GDP, UNRATE, CPIAUCSL)
            start_date: Start date for data
            end_date: End date for data
            frequency: Data frequency (d, w, m, q, a, wef, etc.)
            aggregation_method: Aggregation method (avg, sum, eop)
        
        Returns:
            DataFrame with economic series data
        """
        endpoint = f"{self.endpoints['series']}"
        
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json"
        }
        
        if start_date:
            params["observation_start"] = start_date.strftime('%Y-%m-%d')
        if end_date:
            params["observation_end"] = end_date.strftime('%Y-%m-%d')
        if frequency:
            params["frequency"] = frequency
            params["aggregation_method"] = aggregation_method
        
        logger.info(
            f"Extracting FRED series {series_id}",
            series_id=series_id,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None
        )
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if 'observations' not in data:
            logger.warning(f"No observations returned for series {series_id}", series_id=series_id)
            return pd.DataFrame()
        
        observations = []
        for obs in data['observations']:
            # Skip if value is a dot (.)
            if obs['value'] == '.':
                continue
                
            try:
                obs_data = {
                    "series_id": series_id,
                    "date": pd.to_datetime(obs['date']),
                    "value": float(obs['value']),
                    "realtime_start": pd.to_datetime(obs['realtime_start']),
                    "realtime_end": pd.to_datetime(obs['realtime_end']),
                    "extracted_at": datetime.utcnow()
                }
                observations.append(obs_data)
            except (ValueError, KeyError) as e:
                logger.warning(
                    f"Failed to parse observation for {series_id}",
                    series_id=series_id,
                    date=obs.get('date'),
                    value=obs.get('value'),
                    error=str(e)
                )
                continue
        
        return pd.DataFrame(observations)
    
    def extract_multiple_series(
        self,
        series_ids: List[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Extract multiple economic series
        
        Args:
            series_ids: List of FRED series IDs
            start_date: Start date for data
            end_date: End date for data
        
        Returns:
            Dictionary mapping series_id to DataFrame
        """
        results = {}
        
        for series_id in series_ids:
            try:
                df = self.extract_series(series_id, start_date, end_date)
                results[series_id] = df
                logger.info(
                    f"Extracted {len(df)} records for series {series_id}",
                    series_id=series_id,
                    record_count=len(df)
                )
            except Exception as e:
                logger.error(
                    f"Failed to extract series {series_id}",
                    exc_info=e,
                    series_id=series_id
                )
                results[series_id] = pd.DataFrame()
        
        return results
    
    def search_series(
        self,
        search_text: str,
        limit: int = 100,
        order_by: str = 'popularity',
        sort_order: str = 'desc'
    ) -> pd.DataFrame:
        """
        Search for FRED series
        
        Args:
            search_text: Text to search for
            limit: Maximum number of results
            order_by: Order by field (popularity, title, units, frequency, seasonal_adjustment, etc.)
            sort_order: Sort order (asc, desc)
        
        Returns:
            DataFrame with search results
        """
        endpoint = "/series/search"
        
        params = {
            "search_text": search_text,
            "limit": limit,
            "order_by": order_by,
            "sort_order": sort_order,
            "api_key": self.api_key,
            "file_type": "json"
        }
        
        logger.info(f"Searching FRED for: {search_text}", search_text=search_text, limit=limit)
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if 'seriess' not in data:
            logger.warning(f"No search results for: {search_text}")
            return pd.DataFrame()
        
        search_results = []
        for series in data['seriess']:
            result_data = {
                "series_id": series.get('id'),
                "realtime_start": pd.to_datetime(series.get('realtime_start')),
                "realtime_end": pd.to_datetime(series.get('realtime_end')),
                "title": series.get('title'),
                "observation_start": pd.to_datetime(series.get('observation_start')),
                "observation_end": pd.to_datetime(series.get('observation_end')),
                "frequency": series.get('frequency'),
                "frequency_short": series.get('frequency_short'),
                "units": series.get('units'),
                "units_short": series.get('units_short'),
                "seasonal_adjustment": series.get('seasonal_adjustment'),
                "seasonal_adjustment_short": series.get('seasonal_adjustment_short'),
                "last_updated": pd.to_datetime(series.get('last_updated')),
                "popularity": series.get('popularity'),
                "group_popularity": series.get('group_popularity'),
                "notes": series.get('notes'),
                "extracted_at": datetime.utcnow()
            }
            search_results.append(result_data)
        
        return pd.DataFrame(search_results)
    
    def extract_series_info(
        self,
        series_id: str
    ) -> pd.DataFrame:
        """
        Extract series metadata and information
        
        Args:
            series_id: FRED series ID
        
        Returns:
            DataFrame with series information
        """
        endpoint = "/series"
        
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json"
        }
        
        logger.info(f"Extracting series info for {series_id}", series_id=series_id)
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if 'seriess' not in data or len(data['seriess']) == 0:
            logger.warning(f"No series info found for {series_id}", series_id=series_id)
            return pd.DataFrame()
        
        series_info = data['seriess'][0]
        info_data = {
            "series_id": series_info.get('id'),
            "realtime_start": pd.to_datetime(series_info.get('realtime_start')),
            "realtime_end": pd.to_datetime(series_info.get('realtime_end')),
            "title": series_info.get('title'),
            "observation_start": pd.to_datetime(series_info.get('observation_start')),
            "observation_end": pd.to_datetime(series_info.get('observation_end')),
            "frequency": series_info.get('frequency'),
            "frequency_short": series_info.get('frequency_short'),
            "units": series_info.get('units'),
            "units_short": series_info.get('units_short'),
            "seasonal_adjustment": series_info.get('seasonal_adjustment'),
            "seasonal_adjustment_short": series_info.get('seasonal_adjustment_short'),
            "last_updated": pd.to_datetime(series_info.get('last_updated')),
            "popularity": series_info.get('popularity'),
            "notes": series_info.get('notes'),
            "extracted_at": datetime.utcnow()
        }
        
        return pd.DataFrame([info_data])
    
    def extract_category_series(
        self,
        category_id: int,
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Extract all series in a category
        
        Args:
            category_id: FRED category ID
            limit: Maximum number of series to return
        
        Returns:
            DataFrame with category series
        """
        endpoint = "/category/series"
        
        params = {
            "category_id": category_id,
            "limit": limit,
            "api_key": self.api_key,
            "file_type": "json"
        }
        
        logger.info(f"Extracting series for category {category_id}", category_id=category_id)
        
        response = self._make_request(endpoint, params)
        data = response.json()
        
        if 'seriess' not in data:
            logger.warning(f"No series found for category {category_id}")
            return pd.DataFrame()
        
        category_series = []
        for series in data['seriess']:
            series_data = {
                "series_id": series.get('id'),
                "category_id": category_id,
                "title": series.get('title'),
                "frequency": series.get('frequency'),
                "units": series.get('units'),
                "seasonal_adjustment": series.get('seasonal_adjustment'),
                "realtime_start": pd.to_datetime(series.get('realtime_start')),
                "realtime_end": pd.to_datetime(series.get('realtime_end')),
                "observation_start": pd.to_datetime(series.get('observation_start')),
                "observation_end": pd.to_datetime(series.get('observation_end')),
                "popularity": series.get('popularity'),
                "extracted_at": datetime.utcnow()
            }
            category_series.append(series_data)
        
        return pd.DataFrame(category_series)
    
    def _parse_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse FRED API response into DataFrame
        """
        # FRED responses vary by endpoint, so this is handled by specific methods
        # Return empty DataFrame as fallback
        return pd.DataFrame()
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET"
    ) -> requests.Response:
        """
        Override to handle FRED-specific API requirements
        """
        # FRED requires file_type parameter
        if params is None:
            params = {}
        
        if 'file_type' not in params:
            params['file_type'] = 'json'
        
        return super()._make_request(endpoint, params, method)