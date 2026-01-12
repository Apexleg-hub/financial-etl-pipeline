

# examples/twelve_data_example.py
import pandas as pd
from datetime import datetime, timedelta
from src.extract.twelve_data.factory import TwelveDataExtractorFactory, AssetType


def extract_all_markets():
    """
    Example: Extract data for all market types
    """
    # Create extractors
    extractors = TwelveDataExtractorFactory.create_all_extractors()
    
    all_data = {}
    
    # Extract Forex data
    forex_extractor = extractors[AssetType.FOREX]
    forex_data = forex_extractor.extract_major_pairs(
        interval="1day",
        output_size=100
    )
    all_data['forex'] = forex_data
    
    # Extract Stock data
    stock_extractor = extractors[AssetType.STOCK]
    stock_data = stock_extractor.extract_major_stocks(
        interval="1day",
        output_size=100
    )
    all_data['stocks'] = stock_data
    
    # Extract Crypto data
    crypto_extractor = extractors[AssetType.CRYPTO]
    crypto_data = crypto_extractor.extract_major_cryptos(
        quote_currency="USD",
        interval="4h",
        output_size=200
    )
    all_data['crypto'] = crypto_data
    
    # Extract ETF data
    etf_extractor = extractors[AssetType.ETF]
    etf_data = etf_extractor.extract_major_etfs(
        interval="1week",
        output_size=52
    )
    all_data['etfs'] = etf_data
    
    # Extract Index data
    index_extractor = extractors[AssetType.INDEX]
    index_data = index_extractor.extract_major_indices(
        interval="1day",
        output_size=30
    )
    all_data['indices'] = index_data
    
    return all_data


def incremental_extraction_example():
    """
    Example: Incremental data extraction
    """
    # Create stock extractor
    extractor = TwelveDataExtractorFactory.create_extractor(AssetType.STOCK)
    
    # Get last extracted date (from your metadata database)
    last_extracted = datetime.now() - timedelta(days=7)
    
    # Extract new data since last extraction
    new_data = extractor.extract_time_series(
        symbol="AAPL",
        interval="1day",
        start_date=last_extracted.strftime("%Y-%m-%d"),
        incremental_field="datetime",
        last_extracted=last_extracted
    )
    
    return new_data


def batch_processing_example():
    """
    Example: Batch processing with error handling
    """
    # Create forex extractor
    extractor = TwelveDataExtractorFactory.create_extractor(AssetType.FOREX)
    
    # List of Forex pairs to extract
    forex_pairs = [
        "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF",
        "AUD/USD", "USD/CAD", "NZD/USD", "EUR/GBP"
    ]
    
    # Batch extraction
    results = extractor.extract_time_series_batch(
        symbols=forex_pairs,
        interval="1h",
        output_size=24,
        delay=0.5,  # 500ms delay between requests
        continue_on_error=True
    )
    
    # Combine results
    combined = extractor.combine_batch_results(
        results=results,
        add_item_column=True,
        item_column_name="forex_pair"
    )
    
    return combined


def real_time_quotes_example():
    """
    Example: Get real-time quotes
    """
    quotes = {}
    
    # Get stock quote
    stock_extractor = TwelveDataExtractorFactory.create_extractor(AssetType.STOCK)
    apple_quote = stock_extractor.extract_stock_quote("AAPL")
    quotes['AAPL'] = apple_quote
    
    # Get forex quote
    forex_extractor = TwelveDataExtractorFactory.create_extractor(AssetType.FOREX)
    eurusd_quote = forex_extractor.extract_forex_quote("EUR/USD")
    quotes['EUR/USD'] = eurusd_quote
    
    # Get crypto quote
    crypto_extractor = TwelveDataExtractorFactory.create_extractor(AssetType.CRYPTO)
    btc_quote = crypto_extractor.extract_crypto_quote("BTC", "USD")
    quotes['BTC/USD'] = btc_quote
    
    return quotes


if __name__ == "__main__":
    # Run examples
    print("Extracting market data...")
    
    # Example 1: Extract all markets
    market_data = extract_all_markets()
    
    # Example 2: Incremental extraction
    new_data = incremental_extraction_example()
    
    # Example 3: Batch processing
    batch_results = batch_processing_example()
    
    # Example 4: Real-time quotes
    quotes = real_time_quotes_example()
    
    print("Extraction completed successfully!")