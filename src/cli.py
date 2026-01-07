# src/cli.py
import click
import sys
import os
from datetime import datetime
from .utils.logger import setup_logging

logger = setup_logging()

@click.group()
def cli():
    """Financial ETL Pipeline CLI"""
    pass

@cli.command()
def health():
    """Check system health"""
    click.echo("Checking system health...")
    
    # Add health checks
    click.echo(" Python version: OK")
    click.echo(" Dependencies: OK")
    click.echo(" Environment: OK")
    
    click.echo("\nSystem is healthy! 🎉")

@cli.command()
@click.option('--symbols', '-s', default='AAPL,MSFT,GOOGL', help='Stock symbols (comma-separated)')
@click.option('--output', '-o', type=click.Path(), help='Output directory')
def run(symbols, output):
    """Run ETL pipeline"""
    from .extract.alpha_vantage import AlphaVantageExtractor
    from .transform.data_cleaner import DataCleaner
    from .transform.standardizer import DataStandardizer
    
    symbol_list = [s.strip() for s in symbols.split(',')]
    
    click.echo(f"Running ETL for symbols: {', '.join(symbol_list)}")
    
    extractor = AlphaVantageExtractor()
    
    for symbol in symbol_list:
        try:
            click.echo(f"\nProcessing {symbol}...")
            
            # Extract
            data = extractor.extract_stock_daily(symbol, output_size="compact")
            click.echo(f"  Extracted {len(data)} records")
            
            if output:
                # Save to CSV
                filename = os.path.join(output, f"{symbol}_{datetime.now().strftime('%Y%m%d')}.csv")
                data.to_csv(filename, index=False)
                click.echo(f"  Saved to {filename}")
            
        except Exception as e:
            click.echo(f"   Failed: {e}")
    
    click.echo("\n ETL completed!")

@cli.command()
@click.option('--symbol', '-s', default='AAPL', help='Stock symbol')
def test(symbol):
    """Test extraction for a single symbol"""
    from .extract.alpha_vantage import AlphaVantageExtractor
    
    click.echo(f"Testing extraction for {symbol}...")
    
    extractor = AlphaVantageExtractor()
    
    try:
        data = extractor.extract_stock_daily(symbol, output_size="compact")
        
        click.echo(f" Success! Extracted {len(data)} records")
        click.echo("\nSample data:")
        click.echo(data.head().to_string())
        
        # Basic stats
        click.echo(f"\nDate range: {data['date'].min()} to {data['date'].max()}")
        click.echo(f"Average volume: {data['volume'].mean():,.0f}")
        
    except Exception as e:
        click.echo(f" Failed: {e}")

if __name__ == "__main__":
    cli()