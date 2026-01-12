import os
import requests
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("POLYGON_API_KEY")
BASE_URL = "https://api.polygon.io"


class PolygonFreeExtractor:
    """
    Polygon.io Free Tier Extractor
    - Rate limit: 5 calls/minute
    - Handles retries and errors gracefully
    - Returns pandas DataFrames
    - Works for Stocks + Forex
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize with API key from parameter or environment"""
        self.api_key = api_key or API_KEY
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY not set in environment or provided")
        
        self.base_url = BASE_URL
        self.source_name = "polygon"
        self.calls_made = 0
        self.last_call_time = None
        
    def _rate_limit_check(self):
        """Ensure we don't exceed 5 calls/minute"""
        if self.last_call_time:
            elapsed = time.time() - self.last_call_time
            if elapsed < 12:  # 60s / 5 calls = 12s between calls
                sleep_time = 12 - elapsed
                print(f"Rate limiting: sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
        
        self.last_call_time = time.time()
        self.calls_made += 1
    
    # Add this to your class for testing
def _get_mock(self, endpoint: str, params: Dict[str, Any]) -> Dict:
    """Mock response for testing"""
    print(f"Mock call to: {endpoint}")
    # Return sample data structure
    return {
        "results": [
            {
                "t": int(datetime.now().timestamp() * 1000),
                "o": 150.0,
                "h": 155.0,
                "l": 149.0,
                "c": 152.0,
                "v": 1000000,
                "vw": 151.5,
                "n": 5000
            }
        ]
    }
#################################################
    def _get(self, endpoint: str, params: Dict[str, Any], max_retries: int = 3) -> Dict:
        """Make GET request with retry logic"""
        params["apiKey"] = self.api_key
        
        for attempt in range(max_retries):
            try:
                self._rate_limit_check()
                
                response = requests.get(
                    self.base_url + endpoint,
                    params=params,
                    timeout=30
                )
                
                # Handle rate limits
                if response.status_code == 429:
                    wait_time = 65
                    print(f"Rate limit hit (429). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                # Handle errors
                if response.status_code != 200:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    if attempt < max_retries - 1:
                        print(f"Error on attempt {attempt + 1}: {error_msg}. Retrying...")
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    else:
                        raise Exception(error_msg)
                
                return response.json()
                
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    print(f"Timeout on attempt {attempt + 1}. Retrying...")
                    time.sleep(2 ** attempt)
                else:
                    raise
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    print(f"Request error on attempt {attempt + 1}: {e}. Retrying...")
                    time.sleep(2 ** attempt)
                else:
                    raise
        
        raise Exception("Max retries exceeded")
    
    # -----------------------
    # STOCK DATA
    # -----------------------
    
    def get_stock(self, symbol: str, days: int = 180) -> pd.DataFrame:
        """
        Extract daily stock data
        
        Args:
            symbol: Stock ticker (e.g., 'AAPL')
            days: Number of days of historical data (max 730 for free tier)
        
        Returns:
            DataFrame with OHLCV data
        """
        try:
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=days)
            
            endpoint = f"/v2/aggs/ticker/{symbol}/range/1/day/{start:%Y-%m-%d}/{end:%Y-%m-%d}"
            params = {
                "adjusted": "true",
                "sort": "asc",
                "limit": 50000
            }
            
            print(f"Fetching {symbol} stock data from {start:%Y-%m-%d} to {end:%Y-%m-%d}")
            data = self._get(endpoint, params)
            
            # Check if we got results
            if "results" not in data or not data["results"]:
                print(f"Warning: No data returned for {symbol}")
                return pd.DataFrame()
            
            # Parse results
            rows = []
            for bar in data["results"]:
                rows.append({
                    "date": datetime.fromtimestamp(bar["t"] / 1000, tz=timezone.utc),
                    "symbol": symbol,
                    "open": bar["o"],
                    "high": bar["h"],
                    "low": bar["l"],
                    "close": bar["c"],
                    "volume": bar["v"],
                    "vwap": bar.get("vw"),  # Volume weighted average price
                    "transactions": bar.get("n"),  # Number of transactions
                    "source": self.source_name,
                    "extracted_at": datetime.now(timezone.utc)
                })
            
            df = pd.DataFrame(rows)
            
            # Sort by date
            df = df.sort_values("date", ascending=False).reset_index(drop=True)
            
            print(f"✓ Extracted {len(df)} records for {symbol}")
            return df
            
        except Exception as e:
            print(f"✗ Error extracting {symbol}: {e}")
            return pd.DataFrame()
    
    def get_multiple_stocks(self, symbols: List[str], days: int = 180) -> pd.DataFrame:
        """Extract data for multiple stocks and combine into single DataFrame"""
        all_data = []
        
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] Processing {symbol}...")
            df = self.get_stock(symbol, days)
            if not df.empty:
                all_data.append(df)
        
        if not all_data:
            return pd.DataFrame()
        
        combined = pd.concat(all_data, ignore_index=True)
        print(f"\n✓ Total records extracted: {len(combined)}")
        return combined
    
    # -----------------------
    # FOREX DATA
    # -----------------------
    
    def get_forex(self, pair: str, days: int = 14, timeframe: str = "minute") -> pd.DataFrame:
        """
        Extract forex data (FREE TIER: keep days small)
        
        Args:
            pair: Currency pair like 'EUR/USD' or 'EURUSD'
            days: Number of days (keep ≤14 for free tier)
            timeframe: 'minute', 'hour', or 'day'
        
        Returns:
            DataFrame with forex OHLC data
        """
        try:
            # Format symbol for Polygon (C:EURUSD)
           clean_pair = pair.replace("/", "")
        if "/" in pair:
                # Convert "EUR/USD" to "USDEUR" (base/quote -> quotebase)
                base, quote = pair.split("/")
                clean_pair = quote + base
            symbol = f"C:{clean_pair}"
            
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=days)
            
            # Map timeframe to Polygon format
            timeframe_map = {
                
                "day": "1/day"
                "week": "1/week",
                "month": "1/month",
                
            }
            tf = timeframe_map.get(timeframe, "1/day")
            
            endpoint = f"/v2/aggs/ticker/{symbol}/range/{tf}/{start:%Y-%m-%d}/{end:%Y-%m-%d}"
            params = {
                "adjusted": "true",
                "sort": "asc",
                "limit": 50000
            }
            
            print(f"Fetching {pair} forex data ({timeframe}) from {start:%Y-%m-%d}")
            data = self._get(endpoint, params)
            
            if "results" not in data or not data["results"]:
                print(f"Warning: No data returned for {pair}")
                return pd.DataFrame()
            
            # Parse results
            rows = []
            for bar in data["results"]:
                rows.append({
                    "timestamp": datetime.fromtimestamp(bar["t"] / 1000, tz=timezone.utc),
                    "symbol": pair,
                    "open": bar["o"],
                    "high": bar["h"],
                    "low": bar["l"],
                    "close": bar["c"],
                    "volume": bar.get("v", 0),  # Forex might not have volume
                    "vwap": bar.get("vw"),
                    "transactions": bar.get("n"),
                    "source": self.source_name,
                    "extracted_at": datetime.now(timezone.utc)
                })
            
            df = pd.DataFrame(rows)
            df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)
            
            print(f"✓ Extracted {len(df)} records for {pair}")
            return df
            
        except Exception as e:
            print(f"✗ Error extracting {pair}: {e}")
            return pd.DataFrame()
    
    # -----------------------
    # UTILITY METHODS
    # -----------------------
    
    def get_ticker_details(self, symbol: str) -> Dict:
        """Get metadata about a ticker"""
        try:
            endpoint = f"/v3/reference/tickers/{symbol}"
            data = self._get(endpoint, {})
            return data.get("results", {})
        except Exception as e:
            print(f"Error getting ticker details for {symbol}: {e}")
            return {}
    
    def search_tickers(self, query: str, limit: int = 10) -> List[Dict]:
        """Search for tickers by name or symbol"""
        try:
            endpoint = "/v3/reference/tickers"
            params = {
                "search": query,
                "limit": limit,
                "active": "true"
            }
            data = self._get(endpoint, params)
            return data.get("results", [])
        except Exception as e:
            print(f"Error searching tickers: {e}")
            return []


# -----------------------
# EXAMPLE USAGE
# -----------------------

if __name__ == "__main__":
    # Initialize extractor
    extractor = PolygonFreeExtractor()
    
    # Test 1: Get single stock
    print("\n" + "="*60)
    print("TEST 1: Single Stock")
    print("="*60)
    aapl_df = extractor.get_stock("AAPL", days=30)
    if not aapl_df.empty:
        print(f"\nFirst 5 rows:\n{aapl_df.head()}")
    
    # Test 2: Get multiple stocks
    print("\n" + "="*60)
    print("TEST 2: Multiple Stocks")
    print("="*60)
    stocks_df = extractor.get_multiple_stocks(["AAPL", "MSFT", "GOOGL"], days=30)
    if not stocks_df.empty:
        print(f"\nSymbol counts:\n{stocks_df['symbol'].value_counts()}")
    
    # Test 3: Get forex data
    print("\n" + "="*60)
    print("TEST 3: Forex Data")
    print("="*60)
    forex_df = extractor.get_forex("EUR/USD", days=7, timeframe="hour")
    if not forex_df.empty:
        print(f"\nFirst 5 rows:\n{forex_df.head()}")
    
    print(f"\n\nTotal API calls made: {extractor.calls_made}")