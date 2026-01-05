
# src/extract/base_extractor.py
import abc
import pandas as pd
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from ..utils.logger import logger
from ..utils.rate_limiter import rate_limiter
from config.settings import settings


class BaseExtractor(abc.ABC):
    """Base class for all data extractors"""
    
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.config = settings.load_config("sources")
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "ETL-Pipeline/1.0",
            "Accept": "application/json"
        })
        
    @property
    @abc.abstractmethod
    def api_key(self) -> str:
        """Get API key from settings"""
        pass
    
    @property
    @abc.abstractmethod
    def base_url(self) -> str:
        """Get base URL from config"""
        pass
    
    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((requests.RequestException,)),
        reraise=True
    )
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET"
    ) -> requests.Response:
        """Make HTTP request with rate limiting and retries"""
        # Apply rate limiting
        rate_limiter.wait_if_needed(self.source_name)
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # Add API key to params
        if params is None:
            params = {}
        params["apikey"] = self.api_key
        
        try:
            logger.info(
                f"Making {method} request to {self.source_name}",
                source=self.source_name,
                endpoint=endpoint,
                params={k: v for k, v in params.items() if k != "apikey"}
            )
            
            if method.upper() == "GET":
                response = self.session.get(url, params=params, timeout=30)
            else:
                response = self.session.post(url, json=params, timeout=30)
            
            response.raise_for_status()
            return response
            
        except requests.RequestException as e:
            logger.error(
                f"Request failed for {self.source_name}",
                exc_info=e,
                source=self.source_name,
                endpoint=endpoint,
                status_code=getattr(e.response, 'status_code', None)
            )
            raise
    
    def extract(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        incremental_field: str = "date",
        last_extracted: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Extract data with incremental loading support
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            incremental_field: Field to use for incremental extraction
            last_extracted: Last extraction timestamp
        
        Returns:
            DataFrame with extracted data
        """
        try:
            response = self._make_request(endpoint, params)
            data = response.json()
            
            df = self._parse_response(data)
            
            # Apply incremental filtering
            if last_extracted and incremental_field in df.columns:
                df = df[df[incremental_field] > last_extracted]
                logger.info(
                    f"Extracted {len(df)} new records from {self.source_name}",
                    source=self.source_name,
                    total_records=len(df),
                    last_extracted=last_extracted.isoformat()
                )
            
            return df
            
        except Exception as e:
            logger.error(
                f"Extraction failed for {self.source_name}",
                exc_info=e,
                source=self.source_name
            )
            raise
    
    @abc.abstractmethod
    def _parse_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Parse API response into DataFrame"""
        pass
    
    def get_last_extracted_date(
        self,
        symbol: Optional[str] = None
    ) -> Optional[datetime]:
        """
        Get last extracted date for a symbol (to be implemented with metadata table)
        """
        # This would query the pipeline metadata table
        # For now, return None to extract all data
        return None