# tests/test_polygon.py
import sys
import os
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime, timezone

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.extract.polygon_extractor import PolygonFreeExtractor


def test_initialization():
    """Test that the extractor initializes correctly"""
    print("Testing initialization...")
    
    # Test with API key parameter
    extractor = PolygonFreeExtractor(api_key="test_key_123")
    assert extractor.api_key == "test_key_123"
    assert extractor.base_url == "https://api.polygon.io"
    print("✓ Initialization with API key parameter works")
    
    # Test without API key (should use environment variable)
    # Temporarily set env var
    import os
    os.environ["POLYGON_API_KEY"] = "env_test_key"
    
    # Reload module to pick up env var
    import importlib
    import src.polygon_extractor
    importlib.reload(src.polygon_extractor)
    from src.polygon_extractor import PolygonFreeExtractor
    
    extractor2 = PolygonFreeExtractor()
    assert extractor2.api_key == "env_test_key"
    print("✓ Initialization with env var works")
    
    # Clean up
    del os.environ["POLYGON_API_KEY"]
    print("✓ All initialization tests passed\n")


def test_rate_limiting():
    """Test rate limiting logic"""
    print("Testing rate limiting...")
    
    extractor = PolygonFreeExtractor(api_key="test_key")
    
    # First call should proceed immediately
    extractor._rate_limit_check()
    assert extractor.calls_made == 1
    print("✓ First call passes rate limit")
    
    # Second call immediately should trigger sleep (we'll mock time.sleep)
    with patch('time.sleep') as mock_sleep:
        with patch('time.time', return_value=extractor.last_call_time + 5):
            extractor._rate_limit_check()
            # Should sleep for 12 - 5 = 7 seconds
            mock_sleep.assert_called_once()
            sleep_time = mock_sleep.call_args[0][0]
            assert 6.9 < sleep_time < 7.1  # Check approximately 7 seconds
            print("✓ Rate limiting triggers correct sleep time")
    
    print("✓ Rate limiting tests passed\n")


def test_mock_api_call():
    """Test API call with mocked response"""
    print("Testing mock API call...")
    
    extractor = PolygonFreeExtractor(api_key="test_key")
    
    # Mock the response
    mock_response = {
        "results": [
            {
                "t": 1672531200000,  # Jan 1, 2023
                "o": 150.0,
                "h": 155.0,
                "l": 149.0,
                "c": 152.0,
                "v": 1000000,
                "vw": 151.5,
                "n": 5000
            },
            {
                "t": 1672617600000,  # Jan 2, 2023
                "o": 152.5,
                "h": 154.0,
                "l": 151.0,
                "c": 153.0,
                "v": 1200000,
                "vw": 152.5,
                "n": 5500
            }
        ]
    }
    
    # Mock the requests.get call
    with patch('requests.get') as mock_get:
        # Setup mock response
        mock_response_obj = MagicMock()
        mock_response_obj.status_code = 200
        mock_response_obj.json.return_value = mock_response
        mock_get.return_value = mock_response_obj
        
        # Also mock rate limiting to avoid actual sleep
        with patch.object(extractor, '_rate_limit_check'):
            # Call the method
            df = extractor.get_stock("AAPL", days=2)
            
            # Verify the request was made
            mock_get.assert_called_once()
            
            # Verify DataFrame structure
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert 'symbol' in df.columns
            assert 'open' in df.columns
            assert 'close' in df.columns
            assert df['symbol'].iloc[0] == "AAPL"
            
            print(f"✓ Mock API call successful")
            print(f"✓ DataFrame shape: {df.shape}")
            print(f"✓ Columns: {list(df.columns)}")
    
    print("✓ Mock API tests passed\n")


def test_error_handling():
    """Test error handling in API calls"""
    print("Testing error handling...")
    
    extractor = PolygonFreeExtractor(api_key="test_key")
    
    # Test 429 rate limit response
    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_get.return_value = mock_response
        
        with patch.object(extractor, '_rate_limit_check'):
            with patch('time.sleep') as mock_sleep:
                # This should trigger retry with sleep
                try:
                    extractor._get("/test", {}, max_retries=1)
                except Exception:
                    pass  # Expected to fail after retries
                
                # Should have slept for rate limit
                mock_sleep.assert_called_with(65)
                print("✓ Rate limit (429) handling works")
    
    # Test timeout handling
    with patch('requests.get', side_effect=Exception("Timeout")):
        with patch.object(extractor, '_rate_limit_check'):
            try:
                extractor._get("/test", {}, max_retries=1)
            except Exception as e:
                assert "Timeout" in str(e)
                print("✓ Timeout handling works")
    
    print("✓ Error handling tests passed\n")


def test_forex_symbol_conversion():
    """Test forex symbol conversion logic"""
    print("Testing forex symbol conversion...")
    
    # Note: Your current code has an issue with forex symbols
    # This test will help identify it
    
    extractor = PolygonFreeExtractor(api_key="test_key")
    
    # Test different forex pair formats
    test_cases = [
        ("EUR/USD", "C:EURUSD"),  # Current implementation
        ("USD/JPY", "C:USDJPY"),
        ("EURUSD", "C:EURUSD"),
    ]
    
    for input_pair, expected_symbol in test_cases:
        # We'll test by looking at the endpoint construction
        with patch.object(extractor, '_get') as mock_get:
            mock_get.return_value = {"results": []}
            
            try:
                df = extractor.get_forex(input_pair, days=1)
                # Check what symbol was requested
                call_args = mock_get.call_args
                if call_args:
                    endpoint = call_args[0][0]
                    print(f"  {input_pair} -> {endpoint}")
            except Exception as e:
                print(f"  Error with {input_pair}: {e}")
    
    print("✓ Forex symbol tests completed\n")


def run_all_tests():
    """Run all tests"""
    print("=" * 60)
    print("RUNNING POLYGON EXTRACTOR TESTS")
    print("=" * 60)
    
    tests = [
        test_initialization,
        test_rate_limiting,
        test_mock_api_call,
        test_error_handling,
        test_forex_symbol_conversion,
    ]
    
    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"✗ Test failed: {test.__name__}")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
    
    print("=" * 60)
    print("ALL TESTS COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()