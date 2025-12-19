# src/orchestration/dags/etl_pipeline.py
"""
Complete ETL Pipeline DAG for Apache Airflow
"""
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

from src.extract.alpha_vantage import AlphaVantageExtractor
from src.extract.finnhub import FinnhubExtractor
from src.extract.fred import FREDExtractor
from src.transform.data_cleaner import DataCleaner
from src.transform.feature_engineer import FeatureEngineer
from src.transform.standardizer import DataStandardizer
from src.load.supabase_loader import SupabaseLoader
from src.load.data_models import (
    StockPrice, ForexRate, CryptocurrencyPrice,
    EconomicIndicator, WeatherData, SentimentData
)
from src.utils.logger import logger


# Schema definitions
SCHEMAS = {
    "stock_prices": {
        "symbol": "str",
        "date": "datetime",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "volume": "int",
        "adj_close": "float",
        "dividend_amount": "float",
        "split_coefficient": "float"
    },
    "forex_rates": {
        "from_currency": "str",
        "to_currency": "str",
        "date": "datetime",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float"
    }
}


def extract_stock_data(**context) -> pd.DataFrame:
    """Extract stock data from Alpha Vantage"""
    task_instance = context['ti']
    symbols = context['params'].get('symbols', ['AAPL', 'MSFT', 'GOOGL'])
    
    extractor = AlphaVantageExtractor()
    all_data = []
    
    for symbol in symbols:
        try:
            logger.info(f"Extracting stock data for {symbol}")
            data = extractor.extract_stock_daily(symbol, output_size="full")
            data['symbol'] = symbol
            all_data.append(data)
            
            task_instance.xcom_push(key=f"{symbol}_extracted", value=len(data))
            
        except Exception as e:
            logger.error(f"Failed to extract data for {symbol}", exc_info=e)
            raise AirflowException(f"Extraction failed for {symbol}")
    
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        task_instance.xcom_push(key="stock_data", value=combined_data.to_json())
        return combined_data
    
    raise AirflowException("No data extracted")


def transform_stock_data(**context) -> pd.DataFrame:
    """Transform stock data"""
    task_instance = context['ti']
    
    # Get data from XCom
    data_json = task_instance.xcom_pull(key="stock_data", task_ids="extract_stock_data")
    df = pd.read_json(data_json) if data_json else pd.DataFrame()
    
    if df.empty:
        raise AirflowException("No data to transform")
    
    # Clean data
    cleaner = DataCleaner()
    schema = SCHEMAS["stock_prices"]
    df_clean = cleaner.clean_dataframe(df, schema, "alpha_vantage")
    
    # Engineer features
    engineer = FeatureEngineer()
    df_features = engineer.create_time_series_features(
        df_clean,
        value_column='close',
        date_column='date',
        group_column='symbol'
    )
    
    # Standardize
    standardizer = DataStandardizer()
    df_final = standardizer.standardize_dataframe(df_features, "stock_prices")
    
    # Push to XCom
    task_instance.xcom_push(key="transformed_stock_data", value=df_final.to_json())
    
    return df_final


def load_stock_data(**context) -> Dict[str, Any]:
    """Load stock data to Supabase"""
    task_instance = context['ti']
    run_id = context['run_id']
    
    # Get data from XCom
    data_json = task_instance.xcom_pull(
        key="transformed_stock_data",
        task_ids="transform_stock_data"
    )
    df = pd.read_json(data_json) if data_json else pd.DataFrame()
    
    if df.empty:
        raise AirflowException("No data to load")
    
    # Load to Supabase
    loader = SupabaseLoader()
    results = loader.load_from_dataframe(
        df=df,
        data_model_class=StockPrice,
        pipeline_id="stock_etl",
        run_id=run_id
    )
    
    task_instance.xcom_push(key="load_results", value=results)
    
    return results


def validate_load(**context):
    """Validate the load operation"""
    task_instance = context['ti']
    results = task_instance.xcom_pull(key="load_results", task_ids="load_stock_data")
    
    if not results:
        raise AirflowException("No load results found")
    
    failed_records = results.get('failed', 0)
    error_threshold = context['params'].get('error_threshold', 0.05)
    total_records = results.get('total', 0)
    
    if total_records > 0:
        failure_rate = failed_records / total_records
        
        if failure_rate > error_threshold:
            raise AirflowException(
                f"Load validation failed: {failure_rate:.2%} failure rate "
                f"({failed_records}/{total_records})"
            )
    
    logger.info("Load validation passed", **results)


# Define DAG
default_args = {
    'owner': 'etl_pipeline',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'financial_data_etl',
    default_args=default_args,
    description='ETL pipeline for financial data',
    schedule_interval='0 18 * * 1-5',  # 6 PM UTC on weekdays
    catchup=False,
    tags=['etl', 'financial', 'data'],
    params={
        'symbols': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
        'error_threshold': 0.05
    }
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_stock_data',
    python_callable=extract_stock_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_stock_data',
    python_callable=transform_stock_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_stock_data',
    python_callable=load_stock_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_load',
    python_callable=validate_load,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task >> validate_task