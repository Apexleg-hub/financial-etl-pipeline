# src/utils/logger.py
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional
from pythonjsonlogger import jsonlogger
from ..config.settings import settings


class StructuredLogger:
    """Structured JSON logger for ETL pipeline"""
    
    def __init__(self, name: str = "etl_pipeline"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, settings.log_level))
        
        # Create console handler with JSON formatter
        handler = logging.StreamHandler()
        formatter = jsonlogger.JsonFormatter(
            '%(asctime)s %(levelname)s %(name)s %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def _create_log_record(
        self,
        level: str,
        message: str,
        extra: Optional[Dict[str, Any]] = None,
        exc_info: Optional[Exception] = None
    ) -> Dict[str, Any]:
        """Create structured log record"""
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "environment": settings.environment,
        }
        
        if extra:
            record.update(extra)
        
        if exc_info:
            record["exception"] = str(exc_info)
            record["exception_type"] = exc_info.__class__.__name__
        
        return record
    
    def info(self, message: str, **kwargs):
        """Log info message with structured data"""
        record = self._create_log_record("INFO", message, kwargs)
        self.logger.info(json.dumps(record))
    
    def error(self, message: str, exc_info: Optional[Exception] = None, **kwargs):
        """Log error message with structured data"""
        record = self._create_log_record("ERROR", message, kwargs, exc_info)
        self.logger.error(json.dumps(record))
    
    def warning(self, message: str, **kwargs):
        """Log warning message with structured data"""
        record = self._create_log_record("WARNING", message, kwargs)
        self.logger.warning(json.dumps(record))
    
    def debug(self, message: str, **kwargs):
        """Log debug message with structured data"""
        record = self._create_log_record("DEBUG", message, kwargs)
        self.logger.debug(json.dumps(record))


# Global logger instance
logger = StructuredLogger()