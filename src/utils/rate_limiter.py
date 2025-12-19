# src/utils/rate_limiter.py
import time
from threading import Lock
from typing import Dict, Optional
from dataclasses import dataclass
from ..utils.logger import logger


@dataclass
class RateLimitConfig:
    """Rate limit configuration"""
    max_requests: int
    time_window: int  # seconds
    retry_delay: int = 60


class RateLimiter:
    """Rate limiter for API calls"""
    
    def __init__(self):
        self.requests: Dict[str, list] = {}
        self.locks: Dict[str, Lock] = {}
        self.configs: Dict[str, RateLimitConfig] = {}
    
    def register_source(self, source_name: str, config: RateLimitConfig):
        """Register rate limit configuration for a source"""
        self.configs[source_name] = config
        self.requests[source_name] = []
        self.locks[source_name] = Lock()
    
    def wait_if_needed(self, source_name: str) -> bool:
        """
        Check rate limit and wait if necessary.
        Returns True if wait occurred, False otherwise.
        """
        if source_name not in self.configs:
            logger.warning(f"No rate limit config for {source_name}")
            return False
        
        config = self.configs[source_name]
        
        with self.locks[source_name]:
            current_time = time.time()
            
            # Remove old requests outside time window
            window_start = current_time - config.time_window
            self.requests[source_name] = [
                req_time for req_time in self.requests[source_name]
                if req_time > window_start
            ]
            
            # Check if we've exceeded the limit
            if len(self.requests[source_name]) >= config.max_requests:
                wait_time = config.retry_delay
                logger.warning(
                    f"Rate limit exceeded for {source_name}. "
                    f"Waiting {wait_time} seconds.",
                    source=source_name,
                    wait_time=wait_time
                )
                time.sleep(wait_time)
                self.requests[source_name] = []  # Reset after waiting
                return True
            
            # Record this request
            self.requests[source_name].append(current_time)
            return False
    
    def reset(self, source_name: str):
        """Reset rate limit counter for a source"""
        if source_name in self.requests:
            with self.locks[source_name]:
                self.requests[source_name] = []


# Global rate limiter instance
rate_limiter = RateLimiter()