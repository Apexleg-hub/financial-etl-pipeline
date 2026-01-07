# run_etl.py
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Type, Dict, Any, Callable, Optional
sys.path.append('.')

import click
from src.utils.logger import setup_logging
from src.extract.alpha_vantage import AlphaVantageExtractor
from src.extract.finnhub import FinnhubExtractor
from src.extract.fred import FREDExtractor
from src.extract.weather import WeatherExtractor
from src.transform.data_cleaner import DataCleaner
from src.transform.standardizer import DataStandardizer
from src.transform.validator import DataValidator
from src.load.supabase_loader import SupabaseLoader
from src.load.data_models import StockPrice, EconomicIndicator, WeatherData, ForexRate
from config.settings import settings
from src.extract.forex_extractor import ForexExtractor
# Add delay between API calls in run_etl.py
import time
time.sleep(12)  # 12 seconds between calls for Alpha Vantage free tier

logger = setup_logging()


def load_pipeline_config() -> Dict[str, Any]:
    """Load pipeline configuration from YAML"""
    return settings.load_config("pipeline_config")


def run_pipeline(
    pipeline_name: str,
    extractor: Any,
    items: Optional[List[str]],
    extract_method: Callable,
    cleaner: DataCleaner,
    standardizer: DataStandardizer,
    validator: DataValidator,
    loader: SupabaseLoader,
    data_model_class: Type,
    schema: Dict[str, str],
    source_type: str,
    source_name: str
) -> bool:
    """
    Generic pipeline runner for ETL operations
    
    Args:
        pipeline_name: Name of the pipeline (for logging)
        extractor: Initialized extractor instance
        items: List of items to process (symbols, cities, indicators)
        extract_method: Method to call on extractor
        cleaner: DataCleaner instance
        standardizer: DataStandardizer instance
        validator: DataValidator instance
        loader: SupabaseLoader instance
        data_model_class: SQLAlchemy model class for loading
        schema: Data validation schema
        source_type: Type of data (stock, weather, economic)
        source_name: Source identifier (alpha_vantage, openweather, etc)
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not items:
        logger.warning(f"No items provided for {pipeline_name} ETL")
        return False
    
    logger.info(f"Starting {pipeline_name} ETL for items: {items}")
    
    all_data = []
    
    for item in items:
        try:
            logger.info(f"Processing {item}...")
            
            # 1. EXTRACT
            raw_data = extract_method(item)
            logger.info(f"  Extracted {len(raw_data)} records for {item}")
            
            if raw_data.empty:
                logger.warning(f"  No data returned for {item}")
                continue
            
            # 2. TRANSFORM
            # Clean
            cleaned_data = cleaner.clean_dataframe(raw_data, schema, source_name)
            
            # Standardize
            standardized_data = standardizer.standardize_dataframe(cleaned_data, source_type, source_name)
            
            # Validate
            validated_data, validation_summary = validator.validate_dataframe(
                standardized_data, source_type, source_name
            )
            
            if not validation_summary.is_valid():
                logger.warning(f"  Validation issues for {item}: {validation_summary.failed_checks} failed checks")
            
            logger.info(f"  Transformed {len(validated_data)} records for {item}")
            
            # Add to collection
            all_data.append(validated_data)
            
        except Exception as e:
            logger.error(f"  Failed to process {item}: {e}")
    
    if all_data:
        # Combine all data
        combined_data = pd.concat(all_data, ignore_index=True)
        
        # 3. LOAD
        try:
            load_result = loader.load_from_dataframe(
                df=combined_data,
                data_model_class=data_model_class,
                pipeline_id=f"{pipeline_name.lower().replace(' ', '_')}_etl",
                run_id=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            
            logger.info(f"{pipeline_name} ETL completed successfully!")
            logger.info(f"  Records processed: {load_result.get('total', 0)}")
            logger.info(f"  Records inserted: {load_result.get('inserted', 0)}")
            logger.info(f"  Records updated: {load_result.get('updated', 0)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load {pipeline_name.lower()} data: {e}")
            return False
    else:
        logger.warning(f"No {pipeline_name.lower()} data to process")
        return False


def run_stock_etl(symbols: Optional[List[str]] = None):
    """Run complete stock ETL pipeline using AlphaVantage"""
    if symbols is None:
        config = load_pipeline_config()
        symbols = config.get("sources", {}).get("alpha_vantage", {}).get("symbols", ["AAPL", "MSFT", "GOOGL"])
    
    # Initialize components
    extractor = AlphaVantageExtractor()
    cleaner = DataCleaner()
    standardizer = DataStandardizer()
    validator = DataValidator()
    loader = SupabaseLoader()
    
    schema = {
        "symbol": "str",
        "date": "datetime",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "volume": "int"
    }
    
    return run_pipeline(
        pipeline_name="Stock",
        extractor=extractor,
        items=symbols,
        extract_method=lambda symbol: extractor.extract_stock_daily(symbol, output_size="compact"),
        cleaner=cleaner,
        standardizer=standardizer,
        validator=validator,
        loader=loader,
        data_model_class=StockPrice,
        schema=schema,
        source_type="stock",
        source_name="alpha_vantage"
    )

def run_weather_etl(cities: Optional[List[str]] = None):
    """Run weather ETL pipeline"""
    if cities is None:
        config = load_pipeline_config()
        cities = config.get("sources", {}).get("weather", {}).get("cities", ["New York", "London", "Tokyo"])
    
    extractor = WeatherExtractor()
    cleaner = DataCleaner()
    standardizer = DataStandardizer()
    validator = DataValidator()
    loader = SupabaseLoader()
    
    schema = {
        "location": "str",
        "timestamp": "datetime",
        "temperature": "float",
        "humidity": "float",
        "pressure": "float",
        "wind_speed": "float"
    }
    
    return run_pipeline(
        pipeline_name="Weather",
        extractor=extractor,
        items=cities,
        extract_method=lambda city: extractor.extract_current_weather(city),
        cleaner=cleaner,
        standardizer=standardizer,
        validator=validator,
        loader=loader,
        data_model_class=WeatherData,
        schema=schema,
        source_type="weather",
        source_name="openweather"
    )


def run_forex_etl(pairs: Optional[List[List[str]]] = None) -> bool:
    """Run forex ETL pipeline"""
    if pairs is None:
        config = load_pipeline_config()
        pairs_config = config.get("sources", {}).get("forex", {}).get("pairs", [["EUR", "USD"], ["GBP", "USD"]])
        pairs = [pair for pair in pairs_config]
    
    extractor = ForexExtractor(api_key=settings.alpha_vantage_api_key)
    cleaner = DataCleaner()
    standardizer = DataStandardizer()
    validator = DataValidator()
    loader = SupabaseLoader()
    
    schema = {
        "from_currency": "str",
        "to_currency": "str",
        "date": "datetime",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float"
    }
    
    logger.info(f"Starting Forex ETL for pairs: {pairs}")
    
    all_forex_data = []
    
    for pair in pairs:
        pair_str = f"{pair[0]}/{pair[1]}"
        try:
            from_sym, to_sym = pair[0], pair[1]
            logger.info(f"Processing {pair_str}...")
            
            # Extract forex data
            raw_data = extractor.extract_forex_data(from_sym, to_sym, timeframe="daily", output_size="compact")
            logger.info(f"  Extracted {len(raw_data)} records for {pair_str}")
            
            if raw_data.empty:
                logger.warning(f"  No data returned for {pair_str}")
                continue
            
            # Add currency columns
            raw_data["from_currency"] = from_sym
            raw_data["to_currency"] = to_sym
            
            # Standardize column names
            if "date" not in raw_data.columns and raw_data.index.name == "date":
                raw_data = raw_data.reset_index()
            
            # Transform
            cleaned_data = cleaner.clean_dataframe(raw_data, schema, "alphavantage_forex")
            standardized_data = standardizer.standardize_dataframe(cleaned_data, "forex", "alphavantage")
            validated_data, validation_summary = validator.validate_dataframe(
                standardized_data, "forex", "alphavantage"
            )
            
            if not validation_summary.is_valid():
                logger.warning(f"  Validation issues for {pair_str}: {validation_summary.failed_checks} failed checks")
            
            logger.info(f"  Transformed {len(validated_data)} records for {pair_str}")
            all_forex_data.append(validated_data)
            
        except Exception as e:
            logger.error(f"  Failed to process {pair_str}: {e}")
    
    if all_forex_data:
        combined_data = pd.concat(all_forex_data, ignore_index=True)
        
        try:
            load_result = loader.load_from_dataframe(
                df=combined_data,
                data_model_class=ForexRate,
                pipeline_id="forex_etl",
                run_id=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            
            logger.info(f"Forex ETL completed successfully!")
            logger.info(f"  Records processed: {load_result.get('total', 0)}")
            logger.info(f"  Records inserted: {load_result.get('inserted', 0)}")
            logger.info(f"  Records updated: {load_result.get('updated', 0)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load forex data: {e}")
            return False
    else:
        logger.warning("No forex data to process")
        return False


def run_fred_etl(indicators: Optional[List[str]] = None) -> bool:
    """Run FRED economic indicators ETL pipeline"""
    if indicators is None:
        config = load_pipeline_config()
        indicators = config.get("sources", {}).get("fred", {}).get("indicators", ["UNRATE", "CPIAUCSL", "PAYEMS"])
    
    try:
        extractor = FREDExtractor(api_key=settings.fred_api_key)
        cleaner = DataCleaner()
        standardizer = DataStandardizer()
        validator = DataValidator()
        loader = SupabaseLoader()
        
        schema = {
            "series_id": "str",
            "date": "datetime",
            "value": "float"
        }
        
        logger.info(f"Starting FRED ETL for indicators: {indicators}")
        
        all_fred_data = []
        
        for indicator in indicators:
            try:
                logger.info(f"Processing {indicator}...")
                
                # Extract
                raw_data = extractor.extract_series(indicator)
                logger.info(f"  Extracted {len(raw_data)} records for {indicator}")
                
                if raw_data.empty:
                    logger.warning(f"  No data returned for {indicator}")
                    continue
                
                # Add series_id column
                raw_data["series_id"] = indicator
                
                # Transform
                cleaned_data = cleaner.clean_dataframe(raw_data, schema, "fred")
                standardized_data = standardizer.standardize_dataframe(cleaned_data, "economic", "fred")
                validated_data, validation_summary = validator.validate_dataframe(
                    standardized_data, "economic", "fred"
                )
                
                if not validation_summary.is_valid():
                    logger.warning(f"  Validation issues for {indicator}: {validation_summary.failed_checks} failed checks")
                
                logger.info(f"  Transformed {len(validated_data)} records for {indicator}")
                all_fred_data.append(validated_data)
                
            except Exception as e:
                logger.error(f"  Failed to process {indicator}: {e}")
        
        if all_fred_data:
            combined_data = pd.concat(all_fred_data, ignore_index=True)
            
            try:
                load_result = loader.load_from_dataframe(
                    df=combined_data,
                    data_model_class=EconomicIndicator,
                    pipeline_id="fred_etl",
                    run_id=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )
                
                logger.info(f"FRED ETL completed successfully!")
                logger.info(f"  Records processed: {load_result.get('total', 0)}")
                logger.info(f"  Records inserted: {load_result.get('inserted', 0)}")
                logger.info(f"  Records updated: {load_result.get('updated', 0)}")
                
                return True
                
            except Exception as e:
                logger.error(f"Failed to load FRED data: {e}")
                return False
        else:
            logger.warning("No FRED data to process")
            return False
            
    except Exception as e:
        logger.error(f"FRED ETL failed: {e}")
        return False


def run_finnhub_etl(symbols: Optional[List[str]] = None) -> bool:
    """Run Finnhub stock data ETL pipeline"""
    if symbols is None:
        config = load_pipeline_config()
        symbols = config.get("sources", {}).get("finnhub", {}).get("symbols", ["AAPL", "MSFT", "GOOGL"])
    
    try:
        extractor = FinnhubExtractor()
        cleaner = DataCleaner()
        standardizer = DataStandardizer()
        validator = DataValidator()
        loader = SupabaseLoader()
        
        schema = {
            "symbol": "str",
            "date": "datetime",
            "open": "float",
            "high": "float",
            "low": "float",
            "close": "float",
            "volume": "int"
        }
        
        logger.info(f"Starting Finnhub ETL for symbols: {symbols}")
        
        all_finnhub_data = []
        
        for symbol in symbols:
            try:
                logger.info(f"Processing {symbol}...")
                
                # Extract - use quote as fallback, then try historical
                try:
                    raw_data = extractor.extract_stock_historical(symbol)
                    logger.info(f"  Extracted {len(raw_data)} historical records for {symbol}")
                except:
                    # Fallback to quote if historical fails
                    quote_data = extractor.extract_stock_quote(symbol)
                    if not quote_data.empty:
                        raw_data = quote_data
                        logger.info(f"  Extracted quote data for {symbol}")
                    else:
                        logger.warning(f"  No data available for {symbol}")
                        continue
                
                if raw_data.empty:
                    logger.warning(f"  No data returned for {symbol}")
                    continue
                
                # Add symbol column
                raw_data["symbol"] = symbol
                
                # Transform
                cleaned_data = cleaner.clean_dataframe(raw_data, schema, "finnhub")
                standardized_data = standardizer.standardize_dataframe(cleaned_data, "stock", "finnhub")
                validated_data, validation_summary = validator.validate_dataframe(
                    standardized_data, "stock", "finnhub"
                )
                
                if not validation_summary.is_valid():
                    logger.warning(f"  Validation issues for {symbol}: {validation_summary.failed_checks} failed checks")
                
                logger.info(f"  Transformed {len(validated_data)} records for {symbol}")
                all_finnhub_data.append(validated_data)
                
            except Exception as e:
                logger.error(f"  Failed to process {symbol}: {e}")
        
        if all_finnhub_data:
            combined_data = pd.concat(all_finnhub_data, ignore_index=True)
            
            try:
                load_result = loader.load_from_dataframe(
                    df=combined_data,
                    data_model_class=StockPrice,
                    pipeline_id="finnhub_etl",
                    run_id=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )
                
                logger.info(f"Finnhub ETL completed successfully!")
                logger.info(f"  Records processed: {load_result.get('total', 0)}")
                logger.info(f"  Records inserted: {load_result.get('inserted', 0)}")
                logger.info(f"  Records updated: {load_result.get('updated', 0)}")
                
                return True
                
            except Exception as e:
                logger.error(f"Failed to load Finnhub data: {e}")
                return False
        else:
            logger.warning("No Finnhub data to process")
            return False
            
    except Exception as e:
        logger.error(f"Finnhub ETL failed: {e}")
        return False


@click.command()
@click.option(
    "--pipelines",
    multiple=True,
    type=click.Choice(["stock", "weather", "fred", "finnhub", "forex", "all"]),
    default=["all"],
    help="Which pipelines to run"
)
@click.option(
    "--symbols",
    multiple=True,
    default=None,
    help="Stock symbols to process (comma-separated or multiple --symbols flags)"
)
@click.option(
    "--cities",
    multiple=True,
    default=None,
    help="Cities for weather data (comma-separated or multiple --cities flags)"
)
@click.option(
    "--indicators",
    multiple=True,
    default=None,
    help="FRED economic indicators (comma-separated or multiple --indicators flags)"
)
def main(pipelines: tuple, symbols: tuple, cities: tuple, indicators: tuple):
    """
    Run Financial ETL Pipeline with configurable options
    
    Examples:
        python run_etl.py --pipelines stock --symbols AAPL MSFT
        python run_etl.py --pipelines all
        python run_etl.py --pipelines weather --cities "New York" London
    """
    print("=" * 60)
    print("Financial ETL Pipeline Runner")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Determine which pipelines to run
    pipelines_to_run = set(pipelines)
    if "all" in pipelines_to_run:
        pipelines_to_run = {"stock", "weather", "fred", "finnhub", "forex"}
    
    # Convert tuples to lists, handle None
    symbols_list = list(symbols) if symbols else None
    cities_list = list(cities) if cities else None
    indicators_list = list(indicators) if indicators else None
    
    results = {}
    
    # Run requested pipelines
    if "stock" in pipelines_to_run:
        print("1. Running Stock ETL...")
        results["stock"] = run_stock_etl(symbols_list)
        print()
    
    if "weather" in pipelines_to_run:
        print("2. Running Weather ETL...")
        results["weather"] = run_weather_etl(cities_list)
        print()
    
    if "fred" in pipelines_to_run:
        print("3. Running FRED ETL...")
        results["fred"] = run_fred_etl(indicators_list)
        print()
    
    if "finnhub" in pipelines_to_run:
        print("4. Running Finnhub ETL...")
        results["finnhub"] = run_finnhub_etl(symbols_list)
        print()
    
    if "forex" in pipelines_to_run:
        print("5. Running Forex ETL...")
        results["forex"] = run_forex_etl()
        print()
    
    # Summary
    print("=" * 60)
    print("ETL RUN SUMMARY")
    print("=" * 60)
    for pipeline, success in sorted(results.items()):
        status = "✓ SUCCESS" if success else "✗ FAILED"
        print(f"{pipeline.capitalize():12s}: {status}")
    print(f"Finished at:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    if any(results.values()):
        print("\nETL pipeline completed with some success!")
    else:
        print("\nETL pipeline had issues. Check logs for details.")

if __name__ == "__main__":
    main()