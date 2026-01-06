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
            data['source'] = self.source.value if isinstance(self.source, DataSource) else self.source
        return data


@dataclass
class StockPrice(BaseModel):
    """Stock price data model"""
    symbol: str = None
    date: datetime = None
    open: float = None
    high: float = None
    low: float = None
    close: float = None
    volume: int = None
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
    from_currency: str = None
    to_currency: str = None
    date: datetime = None
    open: float = None
    high: float = None
    low: float = None
    close: float = None
    
    class Meta:
        table_name = "forex_rates"
        unique_constraint = ["from_currency", "to_currency", "date"]


@dataclass
class CryptocurrencyPrice(BaseModel):
    """Cryptocurrency price data model"""
    symbol: str = None
    exchange: str = None
    timestamp: datetime = None
    open: float = None
    high: float = None
    low: float = None
    close: float = None
    volume: float = None
    quote_volume: Optional[float] = None
    
    class Meta:
        table_name = "crypto_prices"
        unique_constraint = ["symbol", "exchange", "timestamp"]


@dataclass
class EconomicIndicator(BaseModel):
    """Economic indicator data model"""
    series_id: str = None
    date: datetime = None
    value: float = None
    realtime_start: Optional[datetime] = None
    realtime_end: Optional[datetime] = None
    
    class Meta:
        table_name = "economic_indicators"
        unique_constraint = ["series_id", "date"]


@dataclass
class WeatherData(BaseModel):
    """Weather data model"""
    location: str = None
    latitude: float = None
    longitude: float = None
    timestamp: datetime = None
    temperature: float = None
    humidity: float = None
    pressure: float = None
    wind_speed: float = None
    weather_condition: str = None
    
    class Meta:
        table_name = "weather_data"
        unique_constraint = ["location", "timestamp"]


@dataclass
class SentimentData(BaseModel):
    """Sentiment data model"""
    source: str = None
    entity: str = None
    timestamp: datetime = None
    sentiment_score: float = None
    confidence: float = None
    raw_text: Optional[str] = None
    url: Optional[str] = None
    
    class Meta:
        table_name = "sentiment_data"
        unique_constraint = ["source", "entity", "timestamp"]


@dataclass
class PipelineMetadata(BaseModel):
    """Pipeline execution metadata"""
    pipeline_id: str = None
    run_id: str = None
    status: str = None
    start_time: datetime = None
    end_time: Optional[datetime] = None
    records_processed: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    
    class Meta:
        table_name = "pipeline_metadata"
        indexes = [["pipeline_id", "run_id"], ["status"], ["start_time"]]