import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from src.load.data_models import (
    DataSource, BaseModel, StockPrice, ForexRate, CryptocurrencyPrice,
    EconomicIndicator, WeatherData, SentimentData, PipelineMetadata
)
from src.load.supabase_loader import SupabaseLoader


@pytest.fixture
def mock_settings():
    """Mock settings for load module"""
    with patch('src.load.supabase_loader.settings') as mock:
        mock.supabase_url = "https://test.supabase.co"
        mock.supabase_key = "test_key"
        mock.max_retries = 3
        mock.batch_size = 1000
        yield mock


@pytest.fixture
def mock_supabase_client():
    """Mock Supabase client"""
    with patch('src.load.supabase_loader.create_client') as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def supabase_loader(mock_settings, mock_supabase_client):
    """Create Supabase loader with mocked client"""
    with patch('src.load.supabase_loader.settings', mock_settings):
        with patch('src.load.supabase_loader.create_client', return_value=mock_supabase_client):
            loader = SupabaseLoader()
            yield loader


class TestDataModels:
    """Test data model classes"""
    
    def test_base_model_creation(self):
        """Test base model creation"""
        now = datetime.utcnow()
        model = BaseModel(
            id="test_id",
            created_at=now,
            updated_at=now,
            source=DataSource.ALPHA_VANTAGE
        )
        
        assert model.id == "test_id"
        assert model.created_at == now
        assert model.source == DataSource.ALPHA_VANTAGE
    
    def test_base_model_to_dict(self):
        """Test converting base model to dictionary"""
        now = datetime.utcnow()
        model = BaseModel(
            id="test_id",
            created_at=now,
            updated_at=now,
            source=DataSource.ALPHA_VANTAGE
        )
        
        model_dict = model.to_dict()
        
        assert model_dict['id'] == "test_id"
        assert model_dict['source'] == "alpha_vantage"
        assert isinstance(model_dict['created_at'], str)
    
    def test_stock_price_model(self):
        """Test stock price data model"""
        date = datetime(2024, 1, 1)
        stock = StockPrice(
            symbol="AAPL",
            date=date,
            open=150.0,
            high=160.0,
            low=140.0,
            close=155.0,
            volume=1000000,
            adj_close=155.0,
            id="stock_1",
            source=DataSource.ALPHA_VANTAGE
        )
        
        assert stock.symbol == "AAPL"
        assert stock.date == date
        assert stock.close == 155.0
        assert stock.volume == 1000000
        assert stock.Meta.table_name == "stock_prices"
        assert ["symbol", "date"] == stock.Meta.unique_constraint
    
    def test_stock_price_to_dict(self):
        """Test converting stock price to dictionary"""
        date = datetime(2024, 1, 1)
        stock = StockPrice(
            symbol="AAPL",
            date=date,
            open=150.0,
            high=160.0,
            low=140.0,
            close=155.0,
            volume=1000000,
            id="stock_1",
            source=DataSource.ALPHA_VANTAGE
        )
        
        stock_dict = stock.to_dict()
        
        assert stock_dict['symbol'] == "AAPL"
        assert stock_dict['close'] == 155.0
        assert stock_dict['source'] == "alpha_vantage"
    
    def test_forex_rate_model(self):
        """Test forex rate data model"""
        date = datetime(2024, 1, 1)
        forex = ForexRate(
            from_currency="USD",
            to_currency="EUR",
            date=date,
            open=0.92,
            high=0.93,
            low=0.91,
            close=0.925,
            id="forex_1",
            source=DataSource.ALPHA_VANTAGE
        )
        
        assert forex.from_currency == "USD"
        assert forex.to_currency == "EUR"
        assert forex.close == 0.925
        assert forex.Meta.table_name == "forex_rates"
    
    def test_crypto_price_model(self):
        """Test cryptocurrency price data model"""
        ts = datetime(2024, 1, 1, 12, 0, 0)
        crypto = CryptocurrencyPrice(
            symbol="BTCUSDT",
            exchange="binance",
            timestamp=ts,
            open=42000.0,
            high=43000.0,
            low=41000.0,
            close=42500.0,
            volume=1000.5,
            id="crypto_1",
            source=DataSource.BINANCE
        )
        
        assert crypto.symbol == "BTCUSDT"
        assert crypto.exchange == "binance"
        assert crypto.close == 42500.0
        assert crypto.Meta.table_name == "crypto_prices"
    
    def test_economic_indicator_model(self):
        """Test economic indicator data model"""
        date = datetime(2024, 1, 1)
        indicator = EconomicIndicator(
            series_id="GDP",
            date=date,
            value=27360.0,
            id="econ_1",
            source=DataSource.FRED
        )
        
        assert indicator.series_id == "GDP"
        assert indicator.value == 27360.0
        assert indicator.Meta.table_name == "economic_indicators"
    
    def test_weather_data_model(self):
        """Test weather data model"""
        ts = datetime(2024, 1, 1, 12, 0, 0)
        weather = WeatherData(
            location="New York",
            latitude=40.7128,
            longitude=-74.0060,
            timestamp=ts,
            temperature=5.0,
            humidity=65.0,
            pressure=1013.25,
            wind_speed=10.5,
            weather_condition="Cloudy",
            id="weather_1",
            source=DataSource.OPENWEATHER
        )
        
        assert weather.location == "New York"
        assert weather.temperature == 5.0
        assert weather.humidity == 65.0
        assert weather.Meta.table_name == "weather_data"
    
    def test_sentiment_data_model(self):
        """Test sentiment data model"""
        ts = datetime(2024, 1, 1, 12, 0, 0)
        sentiment = SentimentData(
            source="twitter",
            entity="AAPL",
            timestamp=ts,
            sentiment_score=0.75,
            confidence=0.95,
            raw_text="Great news from Apple!",
            id="sent_1",
            created_at=ts
        )
        
        assert sentiment.source == "twitter"
        assert sentiment.entity == "AAPL"
        assert sentiment.sentiment_score == 0.75
        assert sentiment.Meta.table_name == "sentiment_data"
    
    def test_pipeline_metadata_model(self):
        """Test pipeline metadata model"""
        start_time = datetime.utcnow()
        metadata = PipelineMetadata(
            pipeline_id="stock_pipeline",
            run_id="run_001",
            status="completed",
            start_time=start_time,
            end_time=start_time + timedelta(minutes=5)
        )
        
        assert metadata.pipeline_id == "stock_pipeline"
        assert metadata.run_id == "run_001"
        assert metadata.status == "completed"
    
    def test_data_source_enum(self):
        """Test DataSource enum values"""
        assert DataSource.ALPHA_VANTAGE.value == "alpha_vantage"
        assert DataSource.FINNHUB.value == "finnhub"
        assert DataSource.FRED.value == "fred"
        assert DataSource.BINANCE.value == "binance"
        assert DataSource.COINBASE.value == "coinbase"
        assert DataSource.OPENWEATHER.value == "openweather"
        assert DataSource.SENTIMENT.value == "sentiment"


class TestSupabaseLoaderConnection:
    """Test Supabase loader connection"""
    
    def test_loader_initialization(self, mock_settings, mock_supabase_client):
        """Test loader initialization"""
        with patch('src.load.supabase_loader.settings', mock_settings):
            with patch('src.load.supabase_loader.create_client', return_value=mock_supabase_client):
                loader = SupabaseLoader()
                
                assert loader.client is not None
    
    def test_loader_connection_failure(self, mock_settings):
        """Test loader handles connection failure"""
        with patch('src.load.supabase_loader.settings', mock_settings):
            with patch('src.load.supabase_loader.create_client', side_effect=Exception("Connection failed")):
                with pytest.raises(Exception, match="Connection failed"):
                    SupabaseLoader()


class TestSupabaseLoaderUpsert:
    """Test Supabase upsert functionality"""
    
    def test_upsert_data_success(self, supabase_loader, mock_supabase_client):
        """Test successful data upsert"""
        # Mock the table response
        mock_table = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [{"id": 1}, {"id": 2}]
        
        mock_table.upsert.return_value.execute.return_value = mock_response
        mock_supabase_client.table.return_value = mock_table
        
        data = [
            {"symbol": "AAPL", "date": "2024-01-01", "close": 150.0},
            {"symbol": "GOOGL", "date": "2024-01-01", "close": 140.0}
        ]
        
        results = supabase_loader.upsert_data(
            table_name="stock_prices",
            data=data,
            conflict_columns=["symbol", "date"]
        )
        
        assert results["total"] == 2
        assert results["inserted"] == 2
        assert results["failed"] == 0
    
    def test_upsert_data_with_batching(self, supabase_loader, mock_supabase_client):
        """Test upsert with batch processing"""
        mock_table = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [{"id": 1}]
        
        mock_table.upsert.return_value.execute.return_value = mock_response
        mock_supabase_client.table.return_value = mock_table
        
        # Create data larger than batch size
        data = [
            {"symbol": f"SYM{i}", "date": f"2024-01-{i:02d}", "close": 150.0 + i}
            for i in range(1, 5)
        ]
        
        results = supabase_loader.upsert_data(
            table_name="stock_prices",
            data=data,
            conflict_columns=["symbol", "date"],
            batch_size=2  # Small batch size to test batching
        )
        
        assert results["total"] == 4
        assert mock_table.upsert.call_count >= 2  # Should be called twice with batch_size=2
    
    def test_upsert_data_failure(self, supabase_loader, mock_supabase_client):
        """Test upsert failure handling"""
        mock_table = MagicMock()
        mock_table.upsert.return_value.execute.side_effect = Exception("Upsert failed")
        mock_supabase_client.table.return_value = mock_table
        
        data = [{"symbol": "AAPL", "date": "2024-01-01", "close": 150.0}]
        
        results = supabase_loader.upsert_data(
            table_name="stock_prices",
            data=data,
            conflict_columns=["symbol", "date"]
        )
        
        assert results["failed"] == 1
        assert len(results["errors"]) > 0
    
    def test_upsert_data_empty_list(self, supabase_loader):
        """Test upsert with empty data list"""
        results = supabase_loader.upsert_data(
            table_name="stock_prices",
            data=[],
            conflict_columns=["symbol", "date"]
        )
        
        assert results["total"] == 0
        assert results["inserted"] == 0
        assert results["failed"] == 0


class TestSupabaseLoaderFromDataFrame:
    """Test loading from DataFrame"""
    
    def test_load_from_dataframe_success(self, supabase_loader, mock_supabase_client):
        """Test successful loading from DataFrame"""
        # Create sample stock data
        df = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL'],
            'date': [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            'open': [150.0, 140.0],
            'high': [160.0, 145.0],
            'low': [140.0, 135.0],
            'close': [155.0, 142.0],
            'volume': [1000000, 900000]
        })
        
        # Mock upsert response
        mock_table = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [{"id": 1}, {"id": 2}]
        
        mock_table.upsert.return_value.execute.return_value = mock_response
        mock_table.insert.return_value.execute.return_value = mock_response
        mock_supabase_client.table.return_value = mock_table
        
        results = supabase_loader.load_from_dataframe(
            df=df,
            data_model_class=StockPrice,
            pipeline_id="test_pipeline",
            run_id="run_001"
        )
        
        assert results["total"] == 2
        assert results["inserted"] == 2
    
    def test_load_from_dataframe_with_nulls(self, supabase_loader, mock_supabase_client):
        """Test loading DataFrame with null values"""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL'],
            'date': [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            'open': [150.0, None],  # Null value
            'high': [160.0, 145.0],
            'low': [140.0, 135.0],
            'close': [155.0, 142.0],
            'volume': [1000000, 900000],
            'adj_close': [155.0, 142.0]
        })
        
        mock_table = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [{"id": 1}, {"id": 2}]
        
        mock_table.upsert.return_value.execute.return_value = mock_response
        mock_table.insert.return_value.execute.return_value = mock_response
        mock_supabase_client.table.return_value = mock_table
        
        results = supabase_loader.load_from_dataframe(
            df=df,
            data_model_class=StockPrice,
            pipeline_id="test_pipeline",
            run_id="run_001"
        )
        
        assert results["total"] == 2
    
    def test_load_from_dataframe_partial_failure(self, supabase_loader, mock_supabase_client):
        """Test partial failure during loading"""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL'],
            'date': [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            'open': [150.0, 140.0],
            'high': [160.0, 145.0],
            'low': [140.0, 135.0],
            'close': [155.0, 142.0],
            'volume': [1000000, 900000]
        })
        
        mock_table = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [{"id": 1}]  # Only 1 record inserted
        
        mock_table.upsert.return_value.execute.return_value = mock_response
        mock_table.insert.return_value.execute.return_value = mock_response
        mock_supabase_client.table.return_value = mock_table
        
        results = supabase_loader.load_from_dataframe(
            df=df,
            data_model_class=StockPrice,
            pipeline_id="test_pipeline",
            run_id="run_001"
        )
        
        assert results["total"] == 2
        assert results["inserted"] == 1


class TestSupabaseLoaderMetadata:
    """Test pipeline metadata functionality"""
    
    def test_save_pipeline_metadata_success(self, supabase_loader, mock_supabase_client):
        """Test saving pipeline metadata"""
        mock_table = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [{"id": "meta_1"}]
        
        mock_table.insert.return_value.execute.return_value = mock_response
        mock_supabase_client.table.return_value = mock_table
        
        metadata = PipelineMetadata(
            pipeline_id="test_pipeline",
            run_id="run_001",
            status="completed",
            start_time=datetime.utcnow()
        )
        
        supabase_loader._save_pipeline_metadata(metadata)
        
        # Verify table was called with pipeline_metadata
        mock_supabase_client.table.assert_called_with("pipeline_metadata")
        mock_table.insert.assert_called_once()
    
    def test_save_pipeline_metadata_failure(self, supabase_loader, mock_supabase_client):
        """Test handling metadata save failure"""
        mock_table = MagicMock()
        mock_table.insert.return_value.execute.side_effect = Exception("Save failed")
        mock_supabase_client.table.return_value = mock_table
        
        metadata = PipelineMetadata(
            pipeline_id="test_pipeline",
            run_id="run_001",
            status="failed",
            start_time=datetime.utcnow()
        )
        
        # Should not raise, just log warning
        supabase_loader._save_pipeline_metadata(metadata)


class TestSupabaseLoaderQueryOperations:
    """Test query operations"""
    
    def test_get_last_loaded_date_success(self, supabase_loader, mock_supabase_client):
        """Test getting last loaded date"""
        mock_table = MagicMock()
        mock_query = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [{"date": "2024-01-15T12:00:00+00:00"}]
        
        mock_table.select.return_value = mock_query
        mock_query.order.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.execute.return_value = mock_response
        mock_supabase_client.table.return_value = mock_table
        
        last_date = supabase_loader.get_last_loaded_date(
            table_name="stock_prices",
            date_column="date"
        )
        
        assert last_date is not None
        assert isinstance(last_date, datetime)
    
    def test_get_last_loaded_date_no_data(self, supabase_loader, mock_supabase_client):
        """Test getting last loaded date when no data exists"""
        mock_table = MagicMock()
        mock_query = MagicMock()
        mock_response = MagicMock()
        mock_response.data = []  # No data
        
        mock_table.select.return_value = mock_query
        mock_query.order.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.execute.return_value = mock_response
        mock_supabase_client.table.return_value = mock_table
        
        last_date = supabase_loader.get_last_loaded_date(
            table_name="stock_prices",
            date_column="date"
        )
        
        assert last_date is None
    
    def test_get_last_loaded_date_with_filter(self, supabase_loader, mock_supabase_client):
        """Test getting last loaded date with filter conditions"""
        mock_table = MagicMock()
        mock_query = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [{"date": "2024-01-15T12:00:00+00:00"}]
        
        mock_table.select.return_value = mock_query
        mock_query.eq.return_value = mock_query
        mock_query.order.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.execute.return_value = mock_response
        mock_supabase_client.table.return_value = mock_table
        
        last_date = supabase_loader.get_last_loaded_date(
            table_name="stock_prices",
            date_column="date",
            filter_conditions={"symbol": "AAPL"}
        )
        
        assert last_date is not None
        mock_query.eq.assert_called()
    
    def test_get_last_loaded_date_failure(self, supabase_loader, mock_supabase_client):
        """Test handling query failure"""
        mock_table = MagicMock()
        mock_table.select.side_effect = Exception("Query failed")
        mock_supabase_client.table.return_value = mock_table
        
        last_date = supabase_loader.get_last_loaded_date(
            table_name="stock_prices",
            date_column="date"
        )
        
        # Should return None on failure
        assert last_date is None


class TestLoadIntegration:
    """Integration tests for load module"""
    
    def test_full_load_pipeline(self, supabase_loader, mock_supabase_client):
        """Test full load pipeline from DataFrame to database"""
        # Create sample data
        df = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL', 'MSFT'],
            'date': [
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
                datetime(2024, 1, 3)
            ],
            'open': [150.0, 140.0, 380.0],
            'high': [160.0, 145.0, 390.0],
            'low': [140.0, 135.0, 370.0],
            'close': [155.0, 142.0, 385.0],
            'volume': [1000000, 900000, 800000]
        })
        
        # Mock responses
        mock_table = MagicMock()
        mock_query = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [{"id": i} for i in range(3)]
        
        mock_table.upsert.return_value.execute.return_value = mock_response
        mock_table.insert.return_value.execute.return_value = mock_response
        mock_table.select.return_value = mock_query
        mock_query.order.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.execute.return_value = mock_response
        
        mock_supabase_client.table.return_value = mock_table
        
        # Load data
        results = supabase_loader.load_from_dataframe(
            df=df,
            data_model_class=StockPrice,
            pipeline_id="stock_etl",
            run_id="run_20240101"
        )
        
        assert results["total"] == 3
        assert results["inserted"] == 3
        
        # Verify metadata was saved
        assert mock_supabase_client.table.call_count > 1
    
    def test_load_different_data_models(self, supabase_loader, mock_supabase_client):
        """Test loading different data model types"""
        # Setup mock
        mock_table = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [{"id": 1}]
        
        mock_table.upsert.return_value.execute.return_value = mock_response
        mock_table.insert.return_value.execute.return_value = mock_response
        mock_supabase_client.table.return_value = mock_table
        
        # Test with Forex data
        forex_df = pd.DataFrame({
            'from_currency': ['USD'],
            'to_currency': ['EUR'],
            'date': [datetime(2024, 1, 1)],
            'open': [0.92],
            'high': [0.93],
            'low': [0.91],
            'close': [0.925]
        })
        
        results = supabase_loader.load_from_dataframe(
            df=forex_df,
            data_model_class=ForexRate,
            pipeline_id="forex_etl",
            run_id="run_20240101"
        )
        
        assert results["total"] == 1
        # Verify the table name was from ForexRate model
        calls = [call for call in mock_supabase_client.table.call_args_list]
        assert any("forex_rates" in str(call) for call in calls)
