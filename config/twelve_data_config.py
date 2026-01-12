


# config/twelve_data_config.py
import os
from dataclasses import dataclass
from typing import List, Optional
from enum import Enum
from dotenv import load_dotenv

load_dotenv()

class AssetType(Enum):
    FOREX = "forex"
    STOCK = "stock"
    CRYPTO = "crypto"
    ETF = "etf"
    INDEX = "index"

@dataclass
class TwelveDataConfig:
    API_KEY: str = os.getenv("TWELVE_DATA_API_KEY", "")
    BASE_URL: str = "https://api.twelvedata.com"
    RATE_LIMIT_REQUESTS: int = 8  # Free tier: 8 requests/min
    RATE_LIMIT_PERIOD: int = 60  # seconds
    
    # Endpoints
    ENDPOINTS = {
        "time_series": "/time_series",
        "quote": "/quote",
        "symbols": "/stocks",
        "forex_pairs": "/forex_pairs",
        "cryptocurrencies": "/cryptocurrencies",
        "etfs": "/etfs",
        "indices": "/indices"
    }
    
    # Default parameters
    DEFAULT_INTERVAL = "1day"
    DEFAULT_OUTPUT_SIZE = 100