# scripts/quick_test.py
"""
Quick test script for Twelve Data extractors
"""
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# ── Fix Python path FIRST (before any imports from src) ──────────
project_root = Path(__file__).resolve().parent.parent   # → financial-etl-pipeline/
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# NOW import from src (not extract)
from src.extract.twelve_data.factory import TwelveDataExtractorFactory, AssetType

# Load environment variables
load_dotenv()

def quick_test():
    print("\n Quick Test of Twelve Data Extractors")
    print("=" * 50)
    
    # Check for API key
    api_key = os.getenv("TWELVE_DATA_API_KEY")
    
    if not api_key or api_key == "your_actual_api_key_here":
        print(" ERROR: TWELVE_DATA_API_KEY not properly configured")
        print("\nPlease check:")
        print("1. Your .env file exists in the project root")
        print("2. .env file contains: TWELVE_DATA_API_KEY=your_real_key_here")
        print("3. The key is not set to the placeholder value")
        
        # Show current .env path
        env_path = project_root / '.env'
        print(f"\nLooking for .env at: {env_path}")
        if env_path.exists():
            print(f" .env file exists")
            with open(env_path, 'r') as f:
                content = f.read()
                print(f"Content:\n{content}")
        else:
            print(f" .env file NOT found at: {env_path}")
            print("\nCreate a .env file with:")
            print("TWELVE_DATA_API_KEY=your_real_api_key_here")
        
        return
    
    print(f" API Key found (starts with: {api_key[:8]}...)")
    
    # Try to import and create extractors
    try:
        print("\n  Creating extractors...")
        
        # Test factory
        extractors = []
        for asset_type in [AssetType.FOREX, AssetType.STOCK, AssetType.CRYPTO, AssetType.ETF, AssetType.INDEX]:
            try:
                extractor = TwelveDataExtractorFactory.create_extractor(asset_type)
                extractors.append((asset_type.value, extractor))
                print(f" Created {asset_type.value} extractor")
            except Exception as e:
                print(f" Failed to create {asset_type.value} extractor: {e}")
        
        if not extractors:
            print("\n No extractors could be created")
            return
        
        print("\n Testing basic functionality...")
        
        # Use the first successful extractor for testing
        extractor_name, extractor = extractors[0]
        
        # Test getting available intervals
        try:
            intervals = extractor.get_available_intervals()
            print(f" Retrieved available intervals: {len(intervals)} intervals")
            print(f"   Sample: {intervals[:5]}...")
        except Exception as e:
            print(f"  Could not retrieve intervals: {e}")
        
        # Try a simple API call with minimal data
        try:
            # For stock extractor, try getting stock list
            if hasattr(extractor, 'get_stocks_list'):
                stocks_df = extractor.get_stocks_list(country="US")
                print(f" Retrieved {len(stocks_df)} US stocks")
            # For forex extractor, try getting forex pairs
            elif hasattr(extractor, 'get_forex_pairs'):
                pairs_df = extractor.get_forex_pairs()
                print(f" Retrieved {len(pairs_df)} forex pairs")
            else:
                print("  No list method available for this extractor")
                
        except Exception as e:
            print(f"  API call failed: {e}")
            print("   This might be due to:")
            print("   - Invalid API key")
            print("   - Rate limiting")
            print("   - Network issues")
        
        print("\n Quick test completed!")
        
    except ImportError as e:
        print(f" Import error: {e}")
        print("Make sure you're in the correct directory and src is in Python path")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f" Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    quick_test()