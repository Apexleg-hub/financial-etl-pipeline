from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict
from enum import Enum


class DataSource(Enum):
    ALPHA_VANTAGE = "alpha_vantage"
    FINNHUB = "finnhub"
    FRED = "fred"
    BINANCE = "binance"
    COINBASE = "coinbase"
    OPENWEATHER = "openweather"
    SENTIMENT = "sentiment"


@dataclass
class BaseModel:
    """Base model for all data entities"""
    id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    source: Optional[DataSource] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion"""
        data = asdict(self)
        if self.created_at:
            data['created_at'] = self.created_at.isoformat()
        if self.updated_at:
            data['updated_at'] = self.updated_at.isoformat()
        if self.source:
            data['source'] = self.source.value
        return data


@dataclass
class StockPrice(BaseModel):
    """Stock price data model"""
    symbol: str
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    adj_close: Optional[float] = None
    dividend_amount: Optional[float] = None
    split_coefficient: Optional[float] = None
    
    class Meta:
        table_name = "stock_prices"
        unique_constraint = ["symbol", "date"]
        indexes = [
            ["symbol"],
            ["date"],
            ["symbol", "date"]
        ]


@dataclass
class ForexRate(BaseModel):
    """Forex rate data model"""
    from_currency: str
    to_currency: str
    date: datetime
    open: float
    high: float
    low: float
    close: float
    
    class Meta:
        table_name = "forex_rates"
        unique_constraint = ["from_currency", "to_currency", "date"]


@dataclass
class CryptocurrencyPrice(BaseModel):
    """Cryptocurrency price data model"""
    symbol: str
    exchange: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: Optional[float] = None
    
    class Meta:
        table_name = "crypto_prices"
        unique_constraint = ["symbol", "exchange", "timestamp"]


@dataclass
class EconomicIndicator(BaseModel):
    """Economic indicator data model"""
    series_id: str
    date: datetime
    value: float
    realtime_start: Optional[datetime] = None
    realtime_end: Optional[datetime] = None
    
    class Meta:
        table_name = "economic_indicators"
        unique_constraint = ["series_id", "date"]


@dataclass
class WeatherData(BaseModel):
    """Weather data model"""
    location: str
    latitude: float
    longitude: float
    timestamp: datetime
    temperature: float
    humidity: float
    pressure: float
    wind_speed: float
    weather_condition: str
    
    class Meta:
        table_name = "weather_data"
        unique_constraint = ["location", "timestamp"]


@dataclass
class SentimentData(BaseModel):
    """Sentiment data model"""
    source: str
    entity: str
    timestamp: datetime
    sentiment_score: float
    confidence: float
    raw_text: Optional[str] = None
    url: Optional[str] = None
    
    class Meta:
        table_name = "sentiment_data"
        unique_constraint = ["source", "entity", "timestamp"]


@dataclass
class PipelineMetadata(BaseModel):
    """Pipeline execution metadata"""
    pipeline_id: str
    run_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    records_processed: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    source: Optional[str] = None
    
    class Meta:
        table_name = "pipeline_metadata"
        indexes = [["pipeline_id", "run_id"], ["status"], ["start_time"]]