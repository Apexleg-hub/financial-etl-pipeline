# config/settings.py
import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
from dotenv import load_dotenv
from pydantic import BaseSettings, Field, validator


class Settings(BaseSettings):
    """Application settings"""
    
    # API Keys
    alpha_vantage_api_key: str = Field(..., env="ALPHA_VANTAGE_API_KEY")
    finnhub_api_key: str = Field(..., env="FINNHUB_API_KEY")
    fred_api_key: str = Field(..., env="FRED_API_KEY")
    openweather_api_key: str = Field(..., env="OPENWEATHER_API_KEY")
    
    # Database
    supabase_url: str = Field(..., env="SUPABASE_URL")
    supabase_key: str = Field(..., env="SUPABASE_KEY")
    supabase_db_password: str = Field(..., env="SUPABASE_DB_PASSWORD")
    
    # Pipeline Settings
    log_level: str = Field("INFO", env="LOG_LEVEL")
    max_retries: int = Field(3, env="MAX_RETRIES")
    batch_size: int = Field(1000, env="BATCH_SIZE")
    environment: str = Field("development", env="ENVIRONMENT")
    
    # Paths
    project_root: Path = Path(__file__).parent.parent
    config_dir: Path = project_root / "config"
    
    class Config:
        env_file = ".env"
        case_sensitive = False
    
    @validator("log_level")
    def validate_log_level(cls, v):
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()
    
    def load_config(self, config_name: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        config_file = self.config_dir / f"{config_name}.yaml"
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}")
        
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)


settings = Settings()