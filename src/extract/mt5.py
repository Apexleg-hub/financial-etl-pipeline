"""
MT5 Forex Data Extractor

Extracts forex data from MetaTrader 5 with precise timestamp and volume information.
Supports multiple timeframes (M1, M5, M15, M30, H1, H4, D1, W1, MN1).

Requirements:
    pip install MetaTrader5

Setup Instructions:
    1. Install MetaTrader5: pip install MetaTrader5
    2. Launch MT5 terminal (application must be running)
    3. Configure broker and account in MT5
    4. Use this extractor to pull historical data
"""

from typing import Optional, List, Dict, Tuple
from datetime import datetime, timedelta, timezone
import logging
from dataclasses import dataclass

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

from src.extract.base_extractor import BaseExtractor


@dataclass
class MT5Config:
    """Configuration for MT5 connection"""
    path: Optional[str] = None  # Path to MT5 terminal
    login: Optional[int] = None  # Account login
    password: Optional[str] = None  # Account password
    server: Optional[str] = None  # Broker server
    timeout: int = 5000  # Connection timeout in ms


class MT5Extractor(BaseExtractor):
    """
    Extract forex data from MetaTrader 5.
    
    MT5 provides:
    - Tick volume (available on all brokers)
    - Real volume (institutional brokers only)
    - Multiple timeframes (M1 through MN1)
    - Precise timestamps (down to seconds)
    
    Usage:
        extractor = MT5Extractor(
            pairs=[('EUR', 'USD'), ('GBP', 'USD')],
            timeframe='H1',
            broker='IC Markets'
        )
        data = extractor.extract_historical(
            symbol='EURUSD',
            days_back=30
        )
    """
    
    # MT5 Timeframe Mapping
    TIMEFRAME_MAP = {
        'M1': mt5.TIMEFRAME_M1 if mt5 else 1,
        'M5': mt5.TIMEFRAME_M5 if mt5 else 5,
        'M15': mt5.TIMEFRAME_M15 if mt5 else 15,
        'M30': mt5.TIMEFRAME_M30 if mt5 else 30,
        'H1': mt5.TIMEFRAME_H1 if mt5 else 60,
        'H4': mt5.TIMEFRAME_H4 if mt5 else 240,
        'D1': mt5.TIMEFRAME_D1 if mt5 else 1440,
        'W1': mt5.TIMEFRAME_W1 if mt5 else 10080,
        'MN1': mt5.TIMEFRAME_MN1 if mt5 else 43200,
    }
    
    def __init__(
        self,
        pairs: List[Tuple[str, str]],
        timeframe: str = 'D1',
        broker: Optional[str] = None,
        config: Optional[MT5Config] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize MT5 extractor.
        
        Args:
            pairs: List of (from_currency, to_currency) tuples. Example: [('EUR', 'USD')]
            timeframe: Candle timeframe: M1, M5, M15, M30, H1, H4, D1, W1, MN1
            broker: Broker name for reference (e.g., 'IC Markets')
            config: MT5Config object for connection details
            logger: Logger instance
        """
        super().__init__(logger)
        
        if mt5 is None:
            raise ImportError(
                "MetaTrader5 module not installed. "
                "Install with: pip install MetaTrader5"
            )
        
        self.pairs = pairs
        self.timeframe = timeframe
        self.broker = broker or "MT5"
        self.config = config or MT5Config()
        
        if timeframe not in self.TIMEFRAME_MAP:
            raise ValueError(f"Invalid timeframe: {timeframe}. Must be one of {list(self.TIMEFRAME_MAP.keys())}")
        
        self._connected = False
        self._connect()
    
    def _connect(self) -> bool:
        """
        Establish connection to MT5 terminal.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Initialize MetaTrader5
            if not mt5.initialize(
                path=self.config.path,
                login=self.config.login,
                password=self.config.password,
                server=self.config.server,
                timeout=self.config.timeout
            ):
                error = mt5.last_error()
                self.logger.error(f"MT5 initialization failed: {error}")
                return False
            
            self._connected = True
            self.logger.info("Connected to MT5 terminal successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"MT5 connection error: {str(e)}")
            return False
    
    def extract_historical(
        self,
        symbol: str,
        days_back: int = 30,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Dict]:
        """
        Extract historical forex data from MT5.
        
        Args:
            symbol: MT5 symbol (e.g., 'EURUSD', 'GBPUSD')
            days_back: Number of days back to fetch (alternative to start_date)
            start_date: Start date for data extraction (UTC)
            end_date: End date for data extraction (UTC, default: now)
            
        Returns:
            List of dictionaries with OHLCV data
            
        Example:
            data = extractor.extract_historical('EURUSD', days_back=30)
            # Returns:
            # [
            #     {
            #         'symbol': 'EURUSD',
            #         'timestamp': datetime(2026, 1, 7, 12, 0, 0, tzinfo=UTC),
            #         'open': 1.08520,
            #         'high': 1.08620,
            #         'low': 1.08480,
            #         'close': 1.08590,
            #         'volume': 45320,
            #         'timeframe': 'H1'
            #     },
            #     ...
            # ]
        """
        if not self._connected:
            self.logger.error("Not connected to MT5")
            raise RuntimeError("MT5 connection not established")
        
        try:
            # Calculate date range
            if end_date is None:
                end_date = datetime.now(timezone.utc)
            
            if start_date is None:
                start_date = end_date - timedelta(days=days_back)
            
            self.logger.info(
                f"Fetching {symbol} from {start_date} to {end_date} "
                f"(timeframe: {self.timeframe})"
            )
            
            # Get MT5 timeframe constant
            mt5_timeframe = self.TIMEFRAME_MAP[self.timeframe]
            
            # Fetch rates from MT5
            rates = mt5.copy_rates_range(
                symbol,
                mt5_timeframe,
                start_date,
                end_date
            )
            
            if rates is None:
                error = mt5.last_error()
                self.logger.warning(f"No data for {symbol}: {error}")
                return []
            
            if len(rates) == 0:
                self.logger.warning(f"Empty result for {symbol}")
                return []
            
            # Parse MT5 rates into our format
            result = []
            for rate in rates:
                from_ccy, to_ccy = self._parse_symbol(symbol)
                
                result.append({
                    'symbol': symbol,
                    'from_currency': from_ccy,
                    'to_currency': to_ccy,
                    'timestamp': datetime.fromtimestamp(rate[0], tz=timezone.utc),
                    'open': float(rate[1]),
                    'high': float(rate[2]),
                    'low': float(rate[3]),
                    'close': float(rate[4]),
                    'volume': int(rate[5]),  # Tick volume
                    'timeframe': self.timeframe,
                    'broker': self.broker,
                })
            
            self.logger.info(f"Successfully extracted {len(result)} candles for {symbol}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error extracting data for {symbol}: {str(e)}")
            raise
    
    def extract_realtime(
        self,
        symbols: Optional[List[str]] = None,
        interval_seconds: int = 60,
        max_iterations: Optional[int] = None,
    ) -> List[Dict]:
        """
        Extract real-time tick data from MT5 (streaming).
        
        Args:
            symbols: List of symbols to monitor (default: all pairs)
            interval_seconds: Polling interval
            max_iterations: Maximum number of polls (None = infinite)
            
        Yields:
            Dictionary with latest tick data
            
        Example:
            extractor = MT5Extractor(pairs=[('EUR', 'USD')])
            for tick in extractor.extract_realtime(['EURUSD'], max_iterations=100):
                print(f"EUR/USD: {tick['bid']}/{tick['ask']}")
        """
        if not self._connected:
            raise RuntimeError("MT5 connection not established")
        
        symbols = symbols or [self._symbol_from_pair(*pair) for pair in self.pairs]
        iteration = 0
        
        try:
            while max_iterations is None or iteration < max_iterations:
                for symbol in symbols:
                    tick = mt5.symbol_info_tick(symbol)
                    
                    if tick is None:
                        self.logger.warning(f"Could not get tick for {symbol}")
                        continue
                    
                    from_ccy, to_ccy = self._parse_symbol(symbol)
                    
                    yield {
                        'symbol': symbol,
                        'from_currency': from_ccy,
                        'to_currency': to_ccy,
                        'timestamp': datetime.fromtimestamp(tick.time, tz=timezone.utc),
                        'bid': float(tick.bid),
                        'ask': float(tick.ask),
                        'bid_volume': int(tick.bid_volume),
                        'ask_volume': int(tick.ask_volume),
                        'last': float(tick.last),
                        'volume': int(tick.volume),
                        'broker': self.broker,
                    }
                
                iteration += 1
                
        except KeyboardInterrupt:
            self.logger.info("Real-time extraction interrupted by user")
        except Exception as e:
            self.logger.error(f"Error in real-time extraction: {str(e)}")
            raise
    
    def get_symbol_info(self, symbol: str) -> Dict:
        """
        Get detailed symbol information from MT5.
        
        Args:
            symbol: MT5 symbol (e.g., 'EURUSD')
            
        Returns:
            Dictionary with symbol properties (spread, point size, etc.)
        """
        if not self._connected:
            raise RuntimeError("MT5 connection not established")
        
        info = mt5.symbol_info(symbol)
        if info is None:
            self.logger.error(f"Symbol not found: {symbol}")
            return {}
        
        return {
            'symbol': symbol,
            'spread': info.spread,
            'point': info.point,  # Minimum price change
            'digits': info.digits,  # Decimal places
            'bid': info.bid,
            'ask': info.ask,
            'bid_volume': info.bid_volume,
            'ask_volume': info.ask_volume,
            'last_price': info.last,
            'min_volume': info.volume_min,
            'max_volume': info.volume_max,
            'tick_size': info.trade_tick_size,
        }
    
    @staticmethod
    def _parse_symbol(symbol: str) -> Tuple[str, str]:
        """
        Parse MT5 symbol into currency pair.
        
        Examples:
            'EURUSD' -> ('EUR', 'USD')
            'GBPUSD' -> ('GBP', 'USD')
            'XAUUSD' -> ('XAU', 'USD')  # Gold
        """
        # Most pairs are 6 characters (3+3)
        if len(symbol) == 6:
            return symbol[:3], symbol[3:]
        
        # Some symbols are 7+ characters
        # Try common 3-letter codes first
        for i in range(3, min(len(symbol) - 2, 5)):
            from_ccy = symbol[:i]
            to_ccy = symbol[i:]
            if len(from_ccy) >= 3 and len(to_ccy) >= 3:
                return from_ccy, to_ccy
        
        # Fallback: assume 3+3
        return symbol[:3], symbol[3:]
    
    @staticmethod
    def _symbol_from_pair(from_currency: str, to_currency: str) -> str:
        """
        Convert currency pair to MT5 symbol format.
        
        Examples:
            ('EUR', 'USD') -> 'EURUSD'
            ('GBP', 'USD') -> 'GBPUSD'
        """
        return f"{from_currency.upper()}{to_currency.upper()}"
    
    def disconnect(self):
        """Disconnect from MT5 terminal."""
        if mt5:
            mt5.shutdown()
            self._connected = False
            self.logger.info("Disconnected from MT5")
    
    def __del__(self):
        """Cleanup: disconnect on object deletion."""
        self.disconnect()


# Example Usage
if __name__ == "__main__":
    import logging
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize extractor
        extractor = MT5Extractor(
            pairs=[('EUR', 'USD'), ('GBP', 'USD'), ('USD', 'JPY')],
            timeframe='H1',
            broker='IC Markets',
        )
        
        # Extract historical data
        logger.info("Extracting historical data...")
        data = extractor.extract_historical('EURUSD', days_back=7)
        
        if data:
            print(f"\nExtracted {len(data)} candles:")
            for candle in data[:5]:  # Show first 5
                print(f"  {candle['timestamp']}: O={candle['open']:.5f} "
                      f"H={candle['high']:.5f} L={candle['low']:.5f} "
                      f"C={candle['close']:.5f} V={candle['volume']}")
        
        # Get symbol info
        logger.info("\nFetching symbol information...")
        info = extractor.get_symbol_info('EURUSD')
        print(f"Symbol info: {info}")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
    finally:
        extractor.disconnect()
