# src/extract/mt5.py
"""
MT5 Forex Data Extractor - Strategic Timeframes Only (D1, W1, MN1)

Extracts daily, weekly, and monthly forex data from MetaTrader 5.
Optimized for long-term analysis and strategic trading decisions.

Requirements:
    pip install MetaTrader5

Setup Instructions:
    1. Install MetaTrader5: pip install MetaTrader5
    2. Launch MT5 terminal (application must be running)
    3. Configure broker and account in MT5
    4. Use this extractor to pull historical data
    Note: Only strategic timeframes (D1, W1, MN1) are supported.
"""

from typing import Optional, List, Dict, Tuple, Any
from datetime import datetime, timedelta, timezone
import pandas as pd
from dataclasses import dataclass

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

from src.extract.base_extractor import BaseExtractor
from src.utils.logger import logger


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
    Extract forex data from MetaTrader 5 - Strategic Timeframes Only.
    
    Supported Timeframes:
    - D1: Daily candles
    - W1: Weekly candles
    - MN1: Monthly candles
    
    MT5 provides:
    - Tick volume (available on all brokers)
    - Real volume (institutional brokers only)
    - Precise timestamps (down to seconds)
    
    Usage:
        extractor = MT5Extractor(
            pairs=[('EUR', 'USD'), ('GBP', 'USD')],
            timeframe='D1',  # Only D1, W1, or MN1 allowed
            broker='IC Markets'
        )
        data = extractor.extract_historical(
            symbol='EURUSD',
            days_back=365
        )
    """
    
    # MT5 Timeframe Mapping - STRATEGIC TIMEFRAMES ONLY
    TIMEFRAME_MAP = {
        "D1": mt5.TIMEFRAME_D1 if mt5 else 1440,
        "W1": mt5.TIMEFRAME_W1 if mt5 else 10080,
        "MN1": mt5.TIMEFRAME_MN1 if mt5 else 43200,
    }
    
    def __init__(
        self,
        pairs: List[Tuple[str, str]],
        timeframe: str = 'D1',
        broker: Optional[str] = None,
        config: Optional[MT5Config] = None,
    ):
        """
        Initialize MT5 extractor.
        
        Args:
            pairs: List of (from_currency, to_currency) tuples. Example: [('EUR', 'USD')]
            timeframe: Candle timeframe - MUST be one of: D1, W1, MN1
            broker: Broker name for reference (e.g., 'IC Markets')
            config: MT5Config object for connection details
            
        Raises:
            ImportError: If MetaTrader5 module not installed
            ValueError: If invalid timeframe specified
        """
        # Initialize base class with source name
        super().__init__(source_name="mt5")
        
        if mt5 is None:
            raise ImportError(
                "MetaTrader5 module not installed. "
                "Install with: pip install MetaTrader5"
            )
        
        self.pairs = pairs
        self.timeframe = timeframe
        self.broker = broker or "MT5"
        self.config = config or MT5Config()
        
        # Validate timeframe - ONLY strategic timeframes allowed
        if timeframe not in self.TIMEFRAME_MAP:
            raise ValueError(
                f"Only strategic timeframes allowed: {list(self.TIMEFRAME_MAP.keys())}. "
                f"Got: {timeframe}"
            )
        
        self._connected = False
        self._connect()
    
    # Required properties from BaseExtractor
    @property
    def api_key(self) -> str:
        """MT5 doesn't use API keys, return empty string"""
        return ""
    
    @property
    def base_url(self) -> str:
        """MT5 doesn't use HTTP, return empty string"""
        return ""
    
    def _parse_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse MT5 response into DataFrame
        
        Note: This is required by BaseExtractor but MT5 doesn't use
        JSON responses. This method is here for interface compliance.
        
        Args:
            data: Response data (not used for MT5)
            
        Returns:
            Empty DataFrame
        """
        return pd.DataFrame()
    
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
                logger.error(
                    f"MT5 initialization failed: {error}",
                    source="mt5",
                    error_code=error[0] if error else None
                )
                return False
            
            self._connected = True
            logger.info(
                "Connected to MT5 terminal successfully",
                source="mt5",
                broker=self.broker
            )
            return True
            
        except Exception as e:
            logger.error(
                f"MT5 connection error: {str(e)}",
                exc_info=e,
                source="mt5"
            )
            return False
    
    def _normalize_start_date(self, start_date: datetime) -> datetime:
        """
        Normalize start date based on timeframe to align with candle boundaries
        
        Args:
            start_date: Original start date
            
        Returns:
            Normalized start date aligned to timeframe boundary
        """
        if self.timeframe == "D1":
            # Start at beginning of day (00:00:00)
            return start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        elif self.timeframe == "W1":
            # Start at beginning of week (Monday 00:00:00)
            days_to_monday = start_date.weekday()
            start_date = start_date - timedelta(days=days_to_monday)
            return start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        elif self.timeframe == "MN1":
            # Start at beginning of month (1st day, 00:00:00)
            return start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        else:
            # Should never reach here due to validation in __init__
            return start_date
    
    def extract_historical(
        self,
        symbol: str,
        days_back: int = 365,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        ) -> List[Dict]:

        # ------------------------------------------------------------------
        # 1. Enforce configured trading universe
        # ------------------------------------------------------------------
        allowed_symbols = {self._symbol_from_pair(*p) for p in self.pairs}
        if symbol not in allowed_symbols:
            raise ValueError(f"Symbol {symbol} not in configured pairs")

    # ------------------------------------------------------------------
    # 2. Enforce supported timeframes (D1, W1, MN1 only)
    # ------------------------------------------------------------------
        if self.timeframe not in {"D1", "W1", "MN1"}:
            raise ValueError("Only D1, W1 and MN1 timeframes are allowed for ETL")

    # ------------------------------------------------------------------
    # 3. Connection validation
    # ------------------------------------------------------------------
        if not self._connected:
            self.logger.error("Not connected to MT5", extra={"source": "mt5"})
            raise RuntimeError("MT5 connection not established")

    # ------------------------------------------------------------------
    # 4. Resolve date range
    # ------------------------------------------------------------------
        if end_date is None:
            end_date = datetime.now(timezone.utc)

        if start_date is None:
           start_date = end_date - timedelta(days=days_back)

        # Normalize to candle boundary
        start_date = self._normalize_start_date(start_date)

    # ------------------------------------------------------------------
    # 5. Ensure symbol is available in MT5
    # ------------------------------------------------------------------
        if not mt5.symbol_select(symbol, True):
           raise ValueError(f"Symbol not available in MT5: {symbol}")

        self.logger.info(
        "Starting MT5 historical extraction",
            extra={
                "source": "mt5",
                "symbol": symbol,
                "timeframe": self.timeframe,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                },
                )

            # ------------------------------------------------------------------
            # 6. Fetch candles
            # ------------------------------------------------------------------
        rates = mt5.copy_rates_range(
                symbol,
                self.TIMEFRAME_MAP[self.timeframe],
                start_date,
                end_date,
             )
        if rates is None or len(rates) == 0:
                self.logger.warning(
                "No candles returned from MT5",
            extra={"source": "mt5", "symbol": symbol},
                )
        return []

            # ------------------------------------------------------------------
            # 7. Parse into warehouse-grade records
            # ------------------------------------------------------------------
        from_ccy, to_ccy = self._parse_symbol(symbol)
        result = []

        for r in rates:
                result.append({
                "source": "MT5",
               "broker": self.broker,
                "symbol": symbol,
                "from_currency": from_ccy,
                "to_currency": to_ccy,
                "timeframe": self.timeframe,
                "timestamp": datetime.fromtimestamp(int(r[0]), tz=timezone.utc),

                # OHLC
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),

                # Volumes (properly separated)
                "tick_volume": int(r[5]),
                "real_volume": int(r[7]) if len(r) > 7 and r[7] > 0 else None,
                })

        self.logger.info(
            "MT5 extraction completed",
        extra={
            "source": "mt5",
            "symbol": symbol,
            "rows": len(result),
            "timeframe": self.timeframe,
          },
           )

        return result

            
            # Get MT5 timeframe constant
        mt5_timeframe = self.TIMEFRAME_MAP[self.timeframe]
            
            # Ensure symbol is available in MT5
        if not mt5.symbol_select(symbol, True):
                error_msg = f"Symbol not available in MT5: {symbol}"
                logger.error(error_msg, source="mt5", symbol=symbol)
                raise ValueError(error_msg)
            
            # Fetch rates from MT5
        rates = mt5.copy_rates_range(
                symbol,
                mt5_timeframe,
                start_date,
                end_date
            )
            
            # Handle no data scenario
        if rates is None:
                error = mt5.last_error()
                logger.warning(
                    f"No data returned for {symbol}: {error}",
                    source="mt5",
                    symbol=symbol,
                    error_code=error[0] if error else None
                )
                return []
            
        if len(rates) == 0:
                logger.warning(
                    f"Empty result for {symbol}",
                    source="mt5",
                    symbol=symbol
                )
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
                    'tick_volume': int(rate[5]),
                    'real_volume': int(rate[7]) if len(rate) > 7 and rate[7] > 0 else None,
                    'timeframe': self.timeframe,
                    'broker': self.broker,
                     'source': 'MT5'
                })
            
        logger.info(
                f"Successfully extracted {len(result)} candles for {symbol}",
                source="mt5",
                symbol=symbol,
                candle_count=len(result),
                timeframe=self.timeframe
            )
        return result
            
        except Exception as e:
        logger.error(
                f"Error extracting data for {symbol}: {str(e)}",
                exc_info=e,
                source="mt5",
                symbol=symbol
            )
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
            logger.error(
                f"Symbol not found: {symbol}",
                source="mt5",
                symbol=symbol
            )
            return {}
        
        return {
            'symbol': symbol,
            'spread': info.spread,
            'point': info.point,  # Minimum price change
            'digits': info.digits,  # Decimal places
            'bid': info.bid,
            'ask': info.ask,
            'volume': info.volume,
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
        if mt5 and self._connected:
            mt5.shutdown()
            self._connected = False
            logger.info("Disconnected from MT5", source="mt5")
    


# Example Usage
if __name__ == "__main__":
    import logging
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    local_logger = logging.getLogger(__name__)
    
    try:
        # Initialize extractor with DAILY data
        extractor = MT5Extractor(
            pairs=[('EUR', 'USD'), ('GBP', 'USD'), ('USD', 'JPY')],
            timeframe='D1',  # Daily candles
            broker='IC Markets',
        )
        
        # Extract 1 year of daily data
        local_logger.info("Extracting 1 year of daily data...")
        data = extractor.extract_historical('EURUSD', days_back=365)
        
        if data:
            print(f"\nExtracted {len(data)} daily candles:")
            for candle in data[:5]:  # Show first 5
                print(f"  {candle['timestamp'].date()}: "
                      f"O={candle['open']:.5f} "
                      f"H={candle['high']:.5f} "
                      f"L={candle['low']:.5f} "
                      f"C={candle['close']:.5f} "
                      f"V={candle['volume']}")
        
        # Get symbol info
        local_logger.info("\nFetching symbol information...")
        info = extractor.get_symbol_info('EURUSD')
        print(f"\nSymbol info: {info}")
        
        # Test weekly data
        extractor_weekly = MT5Extractor(
            pairs=[('EUR', 'USD')],
            timeframe='W1',  # Weekly candles
            broker='IC Markets',
        )
        
        local_logger.info("\nExtracting 1 year of weekly data...")
        weekly_data = extractor_weekly.extract_historical('EURUSD', days_back=365)
        print(f"\nExtracted {len(weekly_data)} weekly candles")
        
    except ValueError as e:
        local_logger.error(f"Configuration error: {str(e)}")
    except Exception as e:
        local_logger.error(f"Error: {str(e)}")
    finally:
        try:
            extractor.disconnect()
            if 'extractor_weekly' in locals():
                extractor_weekly.disconnect()
        except:
            pass