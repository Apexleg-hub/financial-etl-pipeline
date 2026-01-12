
# config/settings.py
import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


class Settings(BaseSettings):
    """Application settings"""
    
    # API Keys
   
    finnhub_api_key: str = Field(...)
    fred_api_key: str = Field(...)
    openweather_api_key: str = Field(...)
    mt5_login: Optional[str] = Field(...)
    polygon_api_key:Optional[str] =Field(... )  
    twelve_data_api_key: Optional[str] = Field(...)

    

    
    # Database
    supabase_url: str = Field(...)
    supabase_key: str = Field(...)
    supabase_db_password: str = Field(...)
    
    # Pipeline Settings
    log_level: str = "INFO"
    max_retries: int = 3
    batch_size: int = 1000
    environment: str = "development"
    
    # Paths
    project_root: Path = Path(__file__).parent.parent
    config_dir: Path = project_root / "config"
    
    # Pydantic v2 configuration
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"  # This ignores extra fields in .env file
    )
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()
    
    @field_validator("project_root", "config_dir")
    @classmethod
    def validate_paths(cls, v: Path) -> Path:
        """Ensure paths are absolute"""
        return v.resolve()
    
    def load_config(self, config_name: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        config_file = self.config_dir / f"{config_name}.yaml"
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}")
        
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)


# Load environment variables from .env file
load_dotenv()

# Create settings instance
settings = Settings()