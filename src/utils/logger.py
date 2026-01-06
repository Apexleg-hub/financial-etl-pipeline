# src/utils/logger.py
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional
import sys
from config.settings import settings
from datetime import datetime, timezone

class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        log_record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "environment": settings.environment,
        }
        
        # Add extra fields if they exist
        if hasattr(record, 'extra'):
            log_record.update(record.extra)
        
        # Add exception info if present
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
            log_record["exception_type"] = record.exc_info[0].__name__
        
        return json.dumps(log_record, default=str)

class StructuredLogger:
    """Structured JSON logger for ETL pipeline without external dependencies"""
    
    def __init__(self, name: str = "etl_pipeline"):
        self.logger = logging.getLogger(name)
        
        # Set log level
        settings_dict = settings.model_dump()
        log_level = settings_dict.get("log_level", "INFO").upper()
        self.logger.setLevel(getattr(logging, log_level))
        # Remove existing handlers to avoid duplicates
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(JSONFormatter())
        
        # Create file handler (optional)
        file_handler = logging.FileHandler(f"logs/etl_pipeline_{datetime.now().strftime('%Y%m%d')}.log")
        file_handler.setFormatter(JSONFormatter())
        
        # Add handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        
        # Prevent propagation to root logger
        self.logger.propagate = False
    
    def _log_with_extra(
        self,
        level: int,
        message: str,
        extra: Optional[Dict[str, Any]] = None,
        exc_info: Optional[Exception] = None
    ):
        """Internal method to log with extra data"""
        if extra is None:
            extra = {}
        
        # Add execution context if available
        if hasattr(self, 'context'):
            extra.update(self.context)
        
        self.logger.log(level, message, extra=extra, exc_info=exc_info)
    
    def info(self, message: str, **kwargs):
        """Log info message with structured data"""
        self._log_with_extra(logging.INFO, message, kwargs)
    
    def error(self, message: str, exc_info: Optional[Exception] = None, **kwargs):
        """Log error message with structured data"""
        self._log_with_extra(logging.ERROR, message, kwargs, exc_info)
    
    def warning(self, message: str, exc_info: Optional[Exception] = None, **kwargs):
        """Log warning message with structured data"""
        self._log_with_extra(logging.WARNING, message, kwargs, exc_info)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with structured data"""
        self._log_with_extra(logging.DEBUG, message, kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message with structured data"""
        self._log_with_extra(logging.CRITICAL, message, kwargs)
    
    def exception(self, message: str, exc_info: Exception, **kwargs):
        """Log exception with structured data"""
        self._log_with_extra(logging.ERROR, message, kwargs, exc_info)
    
    def set_context(self, **context):
        """Set context that will be included in all subsequent logs"""
        if not hasattr(self, 'context'):
            self.context = {}
        self.context.update(context)
    
    def clear_context(self):
        """Clear logging context"""
        if hasattr(self, 'context'):
            self.context.clear()


# Global logger instance
logger = StructuredLogger()

# Convenience functions
def setup_logging():
    """Setup logging configuration"""
    import os
    os.makedirs("logs", exist_ok=True)
    return logger


def get_logger(name: str) -> StructuredLogger:
    """Get a logger instance with specific name"""
    return StructuredLogger(name)


class LogContext:
    """Context manager for logging with specific context"""
    
    def __init__(self, logger: StructuredLogger, **context):
        self.logger = logger
        self.context = context
        self.original_context = {}
    
    def __enter__(self):
        if hasattr(self.logger, 'context'):
            self.original_context = self.logger.context.copy()
        self.logger.set_context(**self.context)
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.original_context:
            self.logger.context = self.original_context
        else:
            self.logger.clear_context()