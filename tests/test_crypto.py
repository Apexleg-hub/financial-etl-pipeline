import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from src.extract.crypto import CryptoExtractor


@pytest.fixture
def mock_settings():
    """Mock settings for crypto extractor"""
    with patch('src.extract.crypto.settings') as mock:
        mock.binance_api_key = "test_binance_key"
        mock.binance_api_secret = "test_binance_secret"
        mock.coinbase_api_key = "test_coinbase_key"
        mock.coinbase_api_secret = "test_coinbase_secret"
        yield mock


@pytest.fixture
def mock_rate_limiter():
    """Mock rate limiter"""
    with patch('src.extract.crypto.rate_limiter') as mock:
        yield mock


@pytest.fixture
def crypto_extractor_binance(mock_settings, mock_rate_limiter):
    """Create Binance crypto extractor with mocked dependencies"""
    with patch('src.extract.crypto.settings', mock_settings):
        with patch('src.extract.crypto.rate_limiter', mock_rate_limiter):
            with patch.object(CryptoExtractor, '__init__', lambda x, exchange="binance": None):
                extractor = CryptoExtractor("binance")
                # Manually set required attributes
                extractor.source_name = "crypto_binance"
                extractor.exchange = "binance"
                extractor.config = {
                    "sources": {
                        "crypto": {
                            "rate_limit": 1200,
                            "retry_delay": 60,
                            "exchanges": {
                                "binance": {
                                    "base_url": "https://api.binance.com",
                                    "endpoints": {}
                                }
                            }
                        }
                    }
                }
                extractor._base_url = "https://api.binance.com"
                extractor.endpoints = {}
                extractor.api_key_value = "test_binance_key"
                extractor.api_secret = "test_binance_secret"
                extractor.logger = Mock()
                extractor._rate_limit = Mock()
                extractor.session = Mock()
                yield extractor


@pytest.fixture
def crypto_extractor_coinbase(mock_settings, mock_rate_limiter):
    """Create Coinbase crypto extractor with mocked dependencies"""
    with patch('src.extract.crypto.settings', mock_settings):
        with patch('src.extract.crypto.rate_limiter', mock_rate_limiter):
            with patch.object(CryptoExtractor, '__init__', lambda x, exchange="coinbase": None):
                extractor = CryptoExtractor("coinbase")
                # Manually set required attributes
                extractor.source_name = "crypto_coinbase"
                extractor.exchange = "coinbase"
                extractor.config = {
                    "sources": {
                        "crypto": {
                            "rate_limit": 10,
                            "retry_delay": 60,
                            "exchanges": {
                                "coinbase": {
                                    "base_url": "https://api.coinbase.com",
                                    "endpoints": {}
                                }
                            }
                        }
                    }
                }
                extractor._base_url = "https://api.coinbase.com"
                extractor.endpoints = {}
                extractor.api_key_value = "test_coinbase_key"
                extractor.api_secret = "test_coinbase_secret"
                extractor.logger = Mock()
                extractor._rate_limit = Mock()
                extractor.session = Mock()
                yield extractor


class TestCryptoExtractorBinance:
    """Test Binance cryptocurrency extractor"""
    
    def test_extract_klines_success(self, crypto_extractor_binance):
        """Test successful kline extraction from Binance"""
        mock_response = Mock()
        mock_response.json.return_value = [
            [
                1609459200000,  # open_time
                "29000.00",     # open
                "30000.00",     # high
                "28500.00",     # low
                "29500.00",     # close
                "100.5",        # volume
                1609545599999,  # close_time
                "2974500.00",   # quote_asset_volume
                150,            # number_of_trades
                "50.25",        # taker_buy_base_asset_volume
                "1487250.00",   # taker_buy_quote_asset_volume
                "0"             # ignore
            ]
        ]
        
        with patch.object(crypto_extractor_binance, '_make_request', return_value=mock_response):
            df = crypto_extractor_binance._extract_binance_klines(
                "BTCUSDT", "1d", None, None, 1000
            )
            
            assert not df.empty
            assert len(df) == 1
            assert df.iloc[0]['symbol'] == "BTCUSDT"
            assert df.iloc[0]['exchange'] == "binance"
            assert float(df.iloc[0]['open']) == 29000.0
            assert float(df.iloc[0]['close']) == 29500.0
            assert float(df.iloc[0]['volume']) == 100.5
    
    def test_extract_klines_with_time_range(self, crypto_extractor_binance):
        """Test kline extraction with start and end times"""
        mock_response = Mock()
        mock_response.json.return_value = [
            [1609459200000, "29000.00", "30000.00", "28500.00", "29500.00", 
             "100.5", 1609545599999, "2974500.00", 150, "50.25", "1487250.00", "0"]
        ]
        
        with patch.object(crypto_extractor_binance, '_make_request', return_value=mock_response) as mock_make:
            start = datetime(2021, 1, 1)
            end = datetime(2021, 12, 31)
            df = crypto_extractor_binance._extract_binance_klines(
                "ETHUSDT", "1h", start, end, 500
            )
            
            assert not df.empty
            # Verify the request was made with correct params
            call_args = mock_make.call_args
            params = call_args[0][1]
            assert params['symbol'] == "ETHUSDT"
            assert params['interval'] == "1h"
            assert params['limit'] == 500
            assert 'startTime' in params
            assert 'endTime' in params
    
    def test_extract_klines_api_error(self, crypto_extractor_binance):
        """Test kline extraction with API error"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': -1121,
            'msg': 'Invalid symbol'
        }
        
        with patch.object(crypto_extractor_binance, '_make_request', return_value=mock_response):
            df = crypto_extractor_binance._extract_binance_klines(
                "INVALID", "1d", None, None, 1000
            )
            
            assert df.empty
    
    def test_extract_klines_empty_data(self, crypto_extractor_binance):
        """Test kline extraction with no data"""
        mock_response = Mock()
        mock_response.json.return_value = []
        
        with patch.object(crypto_extractor_binance, '_make_request', return_value=mock_response):
            df = crypto_extractor_binance._extract_binance_klines(
                "BTCUSDT", "1d", None, None, 1000
            )
            
            assert df.empty
    
    def test_extract_ticker_single_symbol(self, crypto_extractor_binance):
        """Test ticker extraction for single symbol"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'symbol': 'BTCUSDT',
            'priceChange': '1000.00',
            'priceChangePercent': '3.50',
            'weightedAvgPrice': '29500.00',
            'prevClosePrice': '28500.00',
            'lastPrice': '29500.00',
            'bidPrice': '29499.00',
            'askPrice': '29501.00',
            'openPrice': '29000.00',
            'highPrice': '30000.00',
            'lowPrice': '28500.00',
            'volume': '5000.5',
            'quoteVolume': '147500000.00',
            'openTime': 1609459200000,
            'closeTime': 1609545599999,
            'firstId': 1,
            'lastId': 1000,
            'count': 1000
        }
        
        with patch.object(crypto_extractor_binance, '_make_request', return_value=mock_response):
            df = crypto_extractor_binance._extract_binance_ticker("BTCUSDT")
            
            assert not df.empty
            assert len(df) == 1
            assert df.iloc[0]['symbol'] == 'BTCUSDT'
            assert df.iloc[0]['exchange'] == 'binance'
            assert float(df.iloc[0]['last_price']) == 29500.0
            assert float(df.iloc[0]['price_change_percent']) == 3.50
    
    def test_extract_ticker_all_symbols(self, crypto_extractor_binance):
        """Test ticker extraction for all symbols"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                'symbol': 'BTCUSDT',
                'priceChange': '1000.00',
                'priceChangePercent': '3.50',
                'weightedAvgPrice': '29500.00',
                'prevClosePrice': '28500.00',
                'lastPrice': '29500.00',
                'bidPrice': '29499.00',
                'askPrice': '29501.00',
                'openPrice': '29000.00',
                'highPrice': '30000.00',
                'lowPrice': '28500.00',
                'volume': '5000.5',
                'quoteVolume': '147500000.00',
                'openTime': 1609459200000,
                'closeTime': 1609545599999,
                'firstId': 1,
                'lastId': 1000,
                'count': 1000
            },
            {
                'symbol': 'ETHUSDT',
                'priceChange': '100.00',
                'priceChangePercent': '3.00',
                'weightedAvgPrice': '3500.00',
                'prevClosePrice': '3400.00',
                'lastPrice': '3500.00',
                'bidPrice': '3499.00',
                'askPrice': '3501.00',
                'openPrice': '3400.00',
                'highPrice': '3600.00',
                'lowPrice': '3400.00',
                'volume': '50000.5',
                'quoteVolume': '175000000.00',
                'openTime': 1609459200000,
                'closeTime': 1609545599999,
                'firstId': 1,
                'lastId': 500,
                'count': 500
            }
        ]
        
        with patch.object(crypto_extractor_binance, '_make_request', return_value=mock_response):
            df = crypto_extractor_binance._extract_binance_ticker(None)
            
            assert not df.empty
            assert len(df) == 2
            assert set(df['symbol'].unique()) == {'BTCUSDT', 'ETHUSDT'}
    
    def test_extract_ticker_api_error(self, crypto_extractor_binance):
        """Test ticker extraction with API error"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': -1121,
            'msg': 'Invalid symbol'
        }
        
        with patch.object(crypto_extractor_binance, '_make_request', return_value=mock_response):
            df = crypto_extractor_binance._extract_binance_ticker("INVALID")
            
            assert df.empty
    
    def test_base_url_property(self, crypto_extractor_binance):
        """Test base_url property"""
        assert crypto_extractor_binance.base_url == "https://api.binance.com"
    
    def test_extract_klines_multiple_intervals(self, crypto_extractor_binance):
        """Test kline extraction with different intervals"""
        mock_response = Mock()
        mock_response.json.return_value = [
            [1609459200000, "29000.00", "30000.00", "28500.00", "29500.00", 
             "100.5", 1609545599999, "2974500.00", 150, "50.25", "1487250.00", "0"]
        ]
        
        intervals = ["1m", "5m", "15m", "1h", "4h", "1d", "1w", "1M"]
        
        with patch.object(crypto_extractor_binance, '_make_request', return_value=mock_response):
            for interval in intervals:
                df = crypto_extractor_binance._extract_binance_klines(
                    "BTCUSDT", interval, None, None, 1000
                )
                assert not df.empty
                assert df.iloc[0]['interval'] == interval


class TestCryptoExtractorCoinbase:
    """Test Coinbase cryptocurrency extractor"""
    
    def test_extract_candles_success(self, crypto_extractor_coinbase):
        """Test successful candle extraction from Coinbase"""
        mock_response = Mock()
        mock_response.json.return_value = [
            [1609459200, 28500.00, 30000.00, 29000.00, 29500.00, 100.5]
        ]
        
        with patch.object(crypto_extractor_coinbase, '_make_request', return_value=mock_response):
            df = crypto_extractor_coinbase._extract_coinbase_candles(
                "BTC-USD", "1d", None, None, 1000
            )
            
            assert not df.empty
            assert len(df) == 1
            assert df.iloc[0]['symbol'] == "BTC-USD"
            assert df.iloc[0]['exchange'] == "coinbase"
            assert float(df.iloc[0]['open']) == 29000.0
            assert float(df.iloc[0]['close']) == 29500.0
    
    def test_extract_candles_invalid_interval(self, crypto_extractor_coinbase):
        """Test candle extraction with invalid interval"""
        with pytest.raises(ValueError, match="Unsupported interval for Coinbase"):
            crypto_extractor_coinbase._extract_coinbase_candles(
                "BTC-USD", "invalid", None, None, 1000
            )
    
    def test_extract_candles_with_date_range(self, crypto_extractor_coinbase):
        """Test candle extraction with date range"""
        mock_response = Mock()
        mock_response.json.return_value = [
            [1609459200, 28500.00, 30000.00, 29000.00, 29500.00, 100.5]
        ]
        
        with patch.object(crypto_extractor_coinbase, '_make_request', return_value=mock_response) as mock_make:
            start = datetime(2021, 1, 1)
            end = datetime(2021, 12, 31)
            df = crypto_extractor_coinbase._extract_coinbase_candles(
                "ETH-USD", "1h", start, end, 500
            )
            
            assert not df.empty
            # Verify datetime format in request params
            call_args = mock_make.call_args
            params = call_args[0][1]
            assert 'start' in params
            assert 'end' in params
    
    def test_extract_candles_api_error(self, crypto_extractor_coinbase):
        """Test candle extraction with API error"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'message': 'Invalid product ID'
        }
        
        with patch.object(crypto_extractor_coinbase, '_make_request', return_value=mock_response):
            df = crypto_extractor_coinbase._extract_coinbase_candles(
                "INVALID", "1d", None, None, 1000
            )
            
            assert df.empty
    
    def test_extract_candles_empty_data(self, crypto_extractor_coinbase):
        """Test candle extraction with no data"""
        mock_response = Mock()
        mock_response.json.return_value = []
        
        with patch.object(crypto_extractor_coinbase, '_make_request', return_value=mock_response):
            df = crypto_extractor_coinbase._extract_coinbase_candles(
                "BTC-USD", "1d", None, None, 1000
            )
            
            assert df.empty
    
    def test_extract_ticker_success(self, crypto_extractor_coinbase):
        """Test successful ticker extraction from Coinbase"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'trade_id': 12345,
            'price': '29500.00',
            'size': '0.5',
            'bid': '29499.00',
            'ask': '29501.00',
            'volume': '1000.5',
            'time': '2021-01-01T00:00:00Z'
        }
        
        with patch.object(crypto_extractor_coinbase, '_make_request', return_value=mock_response):
            df = crypto_extractor_coinbase._extract_coinbase_ticker("BTC-USD")
            
            assert not df.empty
            assert len(df) == 1
            assert df.iloc[0]['symbol'] == "BTC-USD"
            assert df.iloc[0]['exchange'] == "coinbase"
            assert float(df.iloc[0]['price']) == 29500.0
    
    def test_extract_ticker_api_error(self, crypto_extractor_coinbase):
        """Test ticker extraction with API error"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'message': 'Invalid product ID'
        }
        
        with patch.object(crypto_extractor_coinbase, '_make_request', return_value=mock_response):
            df = crypto_extractor_coinbase._extract_coinbase_ticker("INVALID")
            
            assert df.empty
    
    def test_extract_ticker_no_timestamp(self, crypto_extractor_coinbase):
        """Test ticker extraction with missing timestamp"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'trade_id': 12345,
            'price': '29500.00',
            'size': '0.5',
            'bid': '29499.00',
            'ask': '29501.00',
            'volume': '1000.5'
        }
        
        with patch.object(crypto_extractor_coinbase, '_make_request', return_value=mock_response):
            df = crypto_extractor_coinbase._extract_coinbase_ticker("BTC-USD")
            
            assert not df.empty
            # Timestamp should be None when missing
            assert pd.isna(df.iloc[0]['time'])
    
    def test_extract_candles_granularity_mapping(self, crypto_extractor_coinbase):
        """Test that all supported intervals map correctly to Coinbase granularity"""
        mock_response = Mock()
        mock_response.json.return_value = [
            [1609459200, 28500.00, 30000.00, 29000.00, 29500.00, 100.5]
        ]
        
        interval_granularity_map = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "6h": 21600,
            "1d": 86400
        }
        
        with patch.object(crypto_extractor_coinbase, '_make_request', return_value=mock_response) as mock_make:
            for interval, expected_gran in interval_granularity_map.items():
                df = crypto_extractor_coinbase._extract_coinbase_candles(
                    "BTC-USD", interval, None, None, 1000
                )
                assert not df.empty
                call_args = mock_make.call_args
                params = call_args[0][1]
                assert params['granularity'] == expected_gran
    
    def test_base_url_property(self, crypto_extractor_coinbase):
        """Test base_url property"""
        assert crypto_extractor_coinbase.base_url == "https://api.coinbase.com"


class TestCryptoExtractorInterface:
    """Test public interface methods"""
    
    def test_extract_klines_routing_binance(self, crypto_extractor_binance):
        """Test extract_klines routes to Binance implementation"""
        mock_response = Mock()
        mock_response.json.return_value = [
            [1609459200000, "29000.00", "30000.00", "28500.00", "29500.00", 
             "100.5", 1609545599999, "2974500.00", 150, "50.25", "1487250.00", "0"]
        ]
        
        with patch.object(crypto_extractor_binance, '_make_request', return_value=mock_response):
            df = crypto_extractor_binance.extract_klines("BTCUSDT", "1d")
            assert not df.empty
    
    def test_extract_klines_unsupported_exchange(self, crypto_extractor_binance):
        """Test extract_klines with unsupported exchange"""
        crypto_extractor_binance.exchange = "unsupported"
        
        with pytest.raises(ValueError, match="Unsupported exchange for klines"):
            crypto_extractor_binance.extract_klines("BTCUSDT", "1d")
    
    def test_extract_ticker_routing_binance(self, crypto_extractor_binance):
        """Test extract_ticker routes to Binance implementation"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'symbol': 'BTCUSDT',
            'priceChange': '1000.00',
            'priceChangePercent': '3.50',
            'weightedAvgPrice': '29500.00',
            'prevClosePrice': '28500.00',
            'lastPrice': '29500.00',
            'bidPrice': '29499.00',
            'askPrice': '29501.00',
            'openPrice': '29000.00',
            'highPrice': '30000.00',
            'lowPrice': '28500.00',
            'volume': '5000.5',
            'quoteVolume': '147500000.00',
            'openTime': 1609459200000,
            'closeTime': 1609545599999,
            'firstId': 1,
            'lastId': 1000,
            'count': 1000
        }
        
        with patch.object(crypto_extractor_binance, '_make_request', return_value=mock_response):
            df = crypto_extractor_binance.extract_ticker("BTCUSDT")
            assert not df.empty
    
    def test_extract_ticker_unsupported_exchange(self, crypto_extractor_binance):
        """Test extract_ticker with unsupported exchange"""
        crypto_extractor_binance.exchange = "unsupported"
        
        with pytest.raises(ValueError, match="Unsupported exchange for ticker"):
            crypto_extractor_binance.extract_ticker("BTCUSDT")
    
    def test_extracted_at_timestamp(self, crypto_extractor_binance):
        """Test that extracted_at timestamp is added to all records"""
        mock_response = Mock()
        mock_response.json.return_value = [
            [1609459200000, "29000.00", "30000.00", "28500.00", "29500.00", 
             "100.5", 1609545599999, "2974500.00", 150, "50.25", "1487250.00", "0"]
        ]
        
        with patch.object(crypto_extractor_binance, '_make_request', return_value=mock_response):
            df = crypto_extractor_binance._extract_binance_klines(
                "BTCUSDT", "1d", None, None, 1000
            )
            
            assert 'extracted_at' in df.columns
            assert not df['extracted_at'].isna().all()
            assert all(isinstance(x, datetime) for x in df['extracted_at'])
