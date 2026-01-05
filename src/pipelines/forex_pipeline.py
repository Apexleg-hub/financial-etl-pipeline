

# src/pipelines/forex_pipeline.py
from src.extract.forex_extractor import ForexExtractor, get_major_currency_pairs
from src.utils.logger import logger
from config.settings import settings
import pandas as pd

class ForexETLPipeline:
    def __init__(self):
        self.extractor = ForexExtractor(api_key=settings.alpha_vantage_api_key)
        self.logger = logger
    
    def run_daily_extraction(self):
        """Main ETL job to run daily"""
        self.logger.info("Starting daily Forex ETL pipeline")
        
        # Get all major pairs (8 pairs)
        pairs = get_major_currency_pairs()
        
        # Extract daily data for all pairs (most important)
        daily_data = {}
        for pair in pairs:
            self.logger.info(f"Extracting daily data for {pair[0]}/{pair[1]}")
            data = self.extractor.get_daily_forex(*pair, output_size="full")
            daily_data[f"{pair[0]}_{pair[1]}"] = data
        
        # Extract weekly for EUR/USD only (most traded pair)
        self.logger.info("Extracting weekly data for EUR/USD")
        weekly_data = self.extractor.get_weekly_forex("EUR", "USD")
        
        # Transform and load
        self._transform_and_load(daily_data, weekly_data)
        
        self.logger.info("Forex ETL pipeline completed")
    
    def run_comprehensive_extraction(self):
        """Run weekly for all timeframes and pairs"""
        self.logger.info("Starting comprehensive Forex extraction")
        
        # Get all major pairs
        pairs = get_major_currency_pairs()
        
        # Use batch method for efficiency
        all_data = self.extractor.get_forex_batch(
            pairs[:4],  # First 4 pairs to stay within limits
            ["daily", "weekly"]
        )
        
        # Transform each pair's data
        for pair_key, timeframes_data in all_data.items():
            self._save_to_database(pair_key, timeframes_data)
    
    def _transform_and_load(self, daily_data, weekly_data):
        """Transform and load to database"""
        # Your transformation logic here
        pass
    
    def get_realtime_rates(self, pairs=None):
        """Get real-time rates for dashboard"""
        if pairs is None:
            pairs = [("EUR", "USD"), ("GBP", "USD")]
        
        rates = {}
        for from_curr, to_curr in pairs:
            rate_data = self.extractor.get_exchange_rate(from_curr, to_curr)
            if rate_data:
                rates[f"{from_curr}/{to_curr}"] = {
                    "rate": rate_data.get("5. Exchange Rate"),
                    "timestamp": rate_data.get("6. Last Refreshed")
                }
        
        return rates


# Usage in your main pipeline
if __name__ == "__main__":
    pipeline = ForexETLPipeline()
    
    # Daily job - extract daily for all pairs
    pipeline.run_daily_extraction()
    
    # For real-time dashboard
    realtime_rates = pipeline.get_realtime_rates()
    print("Current rates:", realtime_rates)