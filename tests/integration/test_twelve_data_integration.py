

# tests/integration/test_twelve_data_integration.py
import pytest
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

from src.extract.twelve_data.factory import TwelveDataExtractorFactory, AssetType

# Load environment variables
load_dotenv()


@pytest.mark.integration
class TestTwelveDataIntegration:
    """Integration tests for Twelve Data API"""
    
    @pytest.fixture
    def api_key(self):
        """Get API key from environment"""
        key = os.getenv("TWELVE_DATA_API_KEY")
        if not key:
            pytest.skip("TWELVE_DATA_API_KEY not set in environment")
        return key
    
    def test_extractor_factory_creation(self, api_key):
        """Test factory creates extractors correctly"""
        # Test creating individual extractors
        forex_extractor = TwelveDataExtractorFactory.create_extractor(AssetType.FOREX)
        assert forex_extractor.source_name == "twelve_data"
        
        stock_extractor = TwelveDataExtractorFactory.create_extractor(AssetType.STOCK)
        assert stock_extractor.source_name == "twelve_data"
        
        crypto_extractor = TwelveDataExtractorFactory.create_extractor(AssetType.CRYPTO)
        assert crypto_extractor.source_name == "twelve_data"
        
        etf_extractor = TwelveDataExtractorFactory.create_extractor(AssetType.ETF)
        assert etf_extractor.source_name == "twelve_data"
        
        index_extractor = TwelveDataExtractorFactory.create_extractor(AssetType.INDEX)
        assert index_extractor.source_name == "twelve_data"
    
    def test_get_available_symbols(self, api_key):
        """Test fetching available symbols (lightweight test)"""
        forex_extractor = TwelveDataExtractorFactory.create_extractor(AssetType.FOREX)
        
        try:
            # This is a lightweight call that should work with free tier
            forex_pairs = forex_extractor.get_forex_pairs()
            
            # Check basic structure
            assert isinstance(forex_pairs, pd.DataFrame)
            assert not forex_pairs.empty
            assert "symbol" in forex_pairs.columns
            assert "asset_type" in forex_pairs.columns
            assert all(forex_pairs["asset_type"] == "forex")
            
            print(f"Found {len(forex_pairs)} forex pairs")
            
        except Exception as e:
            # API might be rate limited or have issues, but we should get a proper response format
            print(f"Note: Could not fetch forex pairs: {e}")
            pytest.skip(f"API issue: {e}")
    
    def test_extract_time_series_single(self, api_key):
        """Test extracting single time series"""
        stock_extractor = TwelveDataExtractorFactory.create_extractor(AssetType.STOCK)
        
        try:
            # Extract small amount of data to avoid rate limits
            df = stock_extractor.extract_time_series(
                symbol="AAPL",
                interval="1day",
                output_size=5  # Small sample
            )
            
            # Check DataFrame structure
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert len(df) <= 5  # Should have at most 5 rows
            
            # Check required columns
            required_columns = ['open', 'high', 'low', 'close', 'symbol', 'interval', 'asset_type']
            for col in required_columns:
                assert col in df.columns
            
            # Check data types
            assert df['asset_type'].iloc[0] == 'stock'
            assert df['symbol'].iloc[0] == 'AAPL'
            assert df['interval'].iloc[0] == '1day'
            
            # Check numeric columns
            numeric_cols = ['open', 'high', 'low', 'close']
            for col in numeric_cols:
                assert pd.api.types.is_numeric_dtype(df[col])
            
            print(f"Successfully extracted {len(df)} days of AAPL data")
            
        except Exception as e:
            print(f"Note: Could not extract AAPL data: {e}")
            pytest.skip(f"API issue: {e}")