

import os
import pytest
from datetime import datetime, timezone

from src.extract.mt5 import MT5Extractor, MT5Config


@pytest.fixture(scope="session")
def mt5_config():
    """
    Loads MT5 credentials from .env for integration testing.
    """
    return MT5Config(
        path=os.getenv("MT5_PATH"),
        login=int(os.getenv("MT5_LOGIN")),
        password=os.getenv("MT5_PASSWORD"),
        server=os.getenv("MT5_SERVER"),
    )


@pytest.fixture(scope="session")
def extractor(mt5_config):
    extractor = MT5Extractor(
        pairs=[("EUR", "USD"), ("GBP", "USD")],
        timeframe="D1",
        broker="TestBroker",
        config=mt5_config,
    )
    yield extractor
    extractor.disconnect()


# -------------------------
# Connectivity
# -------------------------

def test_mt5_connection(extractor):
    assert extractor._connected is True


# -------------------------
# Universe enforcement
# -------------------------

def test_symbol_whitelist_enforced(extractor):
    with pytest.raises(ValueError):
        extractor.extract_historical("USDJPY", days_back=10)


# -------------------------
# Data extraction
# -------------------------

def test_daily_data_extraction(extractor):
    data = extractor.extract_historical("EURUSD", days_back=30)

    assert isinstance(data, list)
    assert len(data) > 0


# -------------------------
# Schema validation
# -------------------------

def test_candle_schema(extractor):
    data = extractor.extract_historical("EURUSD", days_back=10)
    row = data[0]

    required_fields = {
        "symbol",
        "from_currency",
        "to_currency",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "tick_volume",
        "real_volume",
        "timeframe",
        "broker",
        "source",
    }

    assert required_fields.issubset(row.keys())


# -------------------------
# Time ordering & timezone
# -------------------------

def test_timestamps_are_utc_and_ordered(extractor):
    data = extractor.extract_historical("EURUSD", days_back=60)

    timestamps = [row["timestamp"] for row in data]

    # All timestamps must be timezone-aware UTC
    for ts in timestamps:
        assert ts.tzinfo is not None
        assert ts.tzinfo == timezone.utc

    # Must be sorted ascending
    assert timestamps == sorted(timestamps)


# -------------------------
# Timeframe correctness
# -------------------------

def test_timeframe_consistency(extractor):
    data = extractor.extract_historical("EURUSD", days_back=60)

    for row in data:
        assert row["timeframe"] == "D1"


# -------------------------
# Symbol info
# -------------------------

def test_symbol_info(extractor):
    info = extractor.get_symbol_info("EURUSD")

    assert info["symbol"] == "EURUSD"
    assert "spread" in info
    assert "digits" in info
    assert info["digits"] > 0
