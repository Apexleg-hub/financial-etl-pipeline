
# scripts/test_twelve_data_end_to_end.py
#!/usr/bin/env python3
"""
End-to-end test script for Twelve Data extractors
"""
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from extract.twelve_data.factory import TwelveDataExtractorFactory, AssetType
from utils.logger import setup_logger

# Setup logging
logger = setup_logger(__name__)


class TwelveDataTester:
    """End-to-end tester for Twelve Data extractors"""
    
    def __init__(self, test_real_api=True):
        """
        Initialize tester
        
        Args:
            test_real_api: Whether to test with real API calls (requires API key)
        """
        self.test_real_api = test_real_api
        self.results = {}
        
        if test_real_api and not os.getenv("TWELVE_DATA_API_KEY"):
            print("⚠️  WARNING: TWELVE_DATA_API_KEY not set in environment")
            print("   Some tests will be skipped")
            self.test_real_api = False
    
    def run_all_tests(self):
        """Run all tests"""
        print("=" * 60)
        print("TWELVE DATA EXTRACTORS - END TO END TEST")
        print("=" * 60)
        
        self.test_factory()
        self.test_time_series()
        self.test_forex()
        self.test_stocks()
        self.test_crypto()
        self.test_etfs()
        self.test_indices()
        self.test_batch_operations()
        
        self.print_summary()
    
    def test_factory(self):
        """Test extractor factory"""
        print("\n🧪 Testing Extractor Factory...")
        
        try:
            # Test creating all extractors
            extractors = TwelveDataExtractorFactory.create_all_extractors()
            
            assert len(extractors) == 5  # Forex, Stock, Crypto, ETF, Index
            assert AssetType.FOREX in extractors
            assert AssetType.STOCK in extractors
            assert AssetType.CRYPTO in extractors
            assert AssetType.ETF in extractors
            assert AssetType.INDEX in extractors
            
            print("✅ Factory creates all extractors correctly")
            self.results['factory'] = 'PASS'
            
        except Exception as e:
            print(f"❌ Factory test failed: {e}")
            self.results['factory'] = 'FAIL'
    
    def test_time_series(self):
        """Test time series extraction"""
        print("\n📈 Testing Time Series Extraction...")
        
        if not self.test_real_api:
            print("⏭️  Skipping (requires real API)")
            self.results['time_series'] = 'SKIP'
            return
        
        try:
            extractor = TwelveDataExtractorFactory.create_extractor(AssetType.STOCK)
            
            # Test with small sample to avoid rate limits
            df = extractor.extract_time_series(
                symbol="AAPL",
                interval="1day",
                output_size=3
            )
            
            # Validate response
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert len(df) <= 3
            
            required_columns = ['open', 'high', 'low', 'close', 'symbol', 'interval', 'asset_type']
            for col in required_columns:
                assert col in df.columns
            
            print(f"✅ Time series: Extracted {len(df)} days of AAPL data")
            self.results['time_series'] = 'PASS'
            
        except Exception as e:
            print(f"❌ Time series test failed: {e}")
            self.results['time_series'] = 'FAIL'
    
    def test_forex(self):
        """Test forex extraction"""
        print("\n💱 Testing Forex Extraction...")
        
        if not self.test_real_api:
            print("⏭️  Skipping (requires real API)")
            self.results['forex'] = 'SKIP'
            return
        
        try:
            extractor = TwelveDataExtractorFactory.create_extractor(AssetType.FOREX)
            
            # Test getting forex pairs
            pairs_df = extractor.get_forex_pairs()
            assert isinstance(pairs_df, pd.DataFrame)
            print(f"✅ Found {len(pairs_df)} forex pairs")
            
            # Test single pair extraction
            df = extractor.extract_forex_time_series(
                symbol="EUR/USD",
                interval="1day",
                output_size=2
            )
            
            assert isinstance(df, pd.DataFrame)
            assert df['asset_type'].iloc[0] == 'forex'
            print(f"✅ Forex: Extracted {len(df)} days of EUR/USD data")
            
            self.results['forex'] = 'PASS'
            
        except Exception as e:
            print(f"❌ Forex test failed: {e}")
            self.results['forex'] = 'FAIL'
    
    def test_stocks(self):
        """Test stocks extraction"""
        print("\n📊 Testing Stocks Extraction...")
        
        if not self.test_real_api:
            print("⏭️  Skipping (requires real API)")
            self.results['stocks'] = 'SKIP'
            return
        
        try:
            extractor = TwelveDataExtractorFactory.create_extractor(AssetType.STOCK)
            
            # Test stock list
            stocks_df = extractor.get_stocks_list()
            assert isinstance(stocks_df, pd.DataFrame)
            print(f"✅ Found {len(stocks_df)} stocks")
            
            # Test major stocks batch extraction
            results = extractor.extract_major_stocks(
                interval="1day",
                output_size=2
            )
            
            assert isinstance(results, dict)
            assert len(results) > 0
            print(f"✅ Stocks: Extracted {len(results)} major stocks")
            
            self.results['stocks'] = 'PASS'
            
        except Exception as e:
            print(f"❌ Stocks test failed: {e}")
            self.results['stocks'] = 'FAIL'
    
    def test_crypto(self):
        """Test crypto extraction"""
        print("\n₿ Testing Crypto Extraction...")
        
        if not self.test_real_api:
            print("⏭️  Skipping (requires real API)")
            self.results['crypto'] = 'SKIP'
            return
        
        try:
            extractor = TwelveDataExtractorFactory.create_extractor(AssetType.CRYPTO)
            
            # Test crypto list
            crypto_df = extractor.get_cryptocurrencies()
            assert isinstance(crypto_df, pd.DataFrame)
            print(f"✅ Found {len(crypto_df)} cryptocurrencies")
            
            # Test single crypto extraction
            df = extractor.extract_crypto_time_series(
                symbol="BTC",
                quote_currency="USD",
                interval="1day",
                output_size=2
            )
            
            assert isinstance(df, pd.DataFrame)
            assert df['asset_type'].iloc[0] == 'crypto'
            print(f"✅ Crypto: Extracted {len(df)} days of BTC/USD data")
            
            self.results['crypto'] = 'PASS'
            
        except Exception as e:
            print(f"❌ Crypto test failed: {e}")
            self.results['crypto'] = 'FAIL'
    
    def test_etfs(self):
        """Test ETFs extraction"""
        print("\n📋 Testing ETFs Extraction...")
        
        if not self.test_real_api:
            print("⏭️  Skipping (requires real API)")
            self.results['etfs'] = 'SKIP'
            return
        
        try:
            extractor = TwelveDataExtractorFactory.create_extractor(AssetType.ETF)
            
            # Test ETF list
            etfs_df = extractor.get_etfs_list()
            assert isinstance(etfs_df, pd.DataFrame)
            print(f"✅ Found {len(etfs_df)} ETFs")
            
            # Test single ETF extraction
            df = extractor.extract_etf_time_series(
                symbol="SPY",
                interval="1day",
                output_size=2
            )
            
            assert isinstance(df, pd.DataFrame)
            assert df['asset_type'].iloc[0] == 'etf'
            print(f"✅ ETFs: Extracted {len(df)} days of SPY data")
            
            self.results['etfs'] = 'PASS'
            
        except Exception as e:
            print(f"❌ ETFs test failed: {e}")
            self.results['etfs'] = 'FAIL'
    
    def test_indices(self):
        """Test indices extraction"""
        print("\n📉 Testing Indices Extraction...")
        
        if not self.test_real_api:
            print("⏭️  Skipping (requires real API)")
            self.results['indices'] = 'SKIP'
            return
        
        try:
            extractor = TwelveDataExtractorFactory.create_extractor(AssetType.INDEX)
            
            # Test index list
            indices_df = extractor.get_indices_list()
            assert isinstance(indices_df, pd.DataFrame)
            print(f"✅ Found {len(indices_df)} indices")
            
            # Test single index extraction
            df = extractor.extract_index_time_series(
                symbol="DJI",
                interval="1day",
                output_size=2
            )
            
            assert isinstance(df, pd.DataFrame)
            assert df['asset_type'].iloc[0] == 'index'
            print(f"✅ Indices: Extracted {len(df)} days of DJI data")
            
            self.results['indices'] = 'PASS'
            
        except Exception as e:
            print(f"❌ Indices test failed: {e}")
            self.results['indices'] = 'FAIL'
    
    def test_batch_operations(self):
        """Test batch operations"""
        print("\n🔄 Testing Batch Operations...")
        
        try:
            # Use the ALL extractor for generic batch testing
            extractor = TwelveDataExtractorFactory.create_extractor(AssetType.ALL)
            
            # Create mock batch results
            mock_results = {
                'AAPL': pd.DataFrame({'close': [100, 101], 'date': pd.date_range('2024-01-01', periods=2)}),
                'MSFT': pd.DataFrame({'close': [200, 201], 'date': pd.date_range('2024-01-01', periods=2)})
            }
            
            # Test combining
            combined = extractor.combine_batch_results(
                results=mock_results,
                add_item_column=True,
                item_column_name='symbol'
            )
            
            assert isinstance(combined, pd.DataFrame)
            assert len(combined) == 4
            assert 'symbol' in combined.columns
            
            print(f"✅ Batch: Successfully combined {len(mock_results)} DataFrames")
            self.results['batch'] = 'PASS'
            
        except Exception as e:
            print(f"❌ Batch operations test failed: {e}")
            self.results['batch'] = 'FAIL'
    
    def print_summary(self):
        """Print test summary"""
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        
        passed = sum(1 for result in self.results.values() if result == 'PASS')
        failed = sum(1 for result in self.results.values() if result == 'FAIL')
        skipped = sum(1 for result in self.results.values() if result == 'SKIP')
        total = len(self.results)
        
        for test, result in self.results.items():
            status = "✅ PASS" if result == 'PASS' else "❌ FAIL" if result == 'FAIL' else "⏭️  SKIP"
            print(f"{status}: {test}")
        
        print("-" * 60)
        print(f"Total: {total} | Passed: {passed} | Failed: {failed} | Skipped: {skipped}")
        
        if failed == 0:
            print("\n🎉 All tests passed!")
        else:
            print(f"\n⚠️  {failed} test(s) failed")
            sys.exit(1)


def main():
    """Main test runner"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Twelve Data extractors')
    parser.add_argument('--real-api', action='store_true', 
                       help='Test with real API calls (requires API key)')
    parser.add_argument('--no-real-api', dest='real_api', action='store_false',
                       help='Skip real API tests')
    parser.set_defaults(real_api=True)
    
    args = parser.parse_args()
    
    tester = TwelveDataTester(test_real_api=args.real_api)
    tester.run_all_tests()


if __name__ == "__main__":
    main()