@"

import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def fetch_alpha_vantage(**context):
    \"\"\"Fetch stock data from Alpha Vantage\"\"\"
    try:
        logger.info("Starting Alpha Vantage data fetch")
        
        # Get API key - you'll need to set this in Airflow Variables
        # api_key = Variable.get("ALPHA_VANTAGE_API_KEY")
        # For testing, use a demo key or set it here temporarily
        api_key = "demo"  # Replace with your actual API key
        
        symbols = ['IBM', 'AAPL']  # Start with fewer symbols for testing
        
        all_data = []
        
        for symbol in symbols:
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol,
                'apikey': api_key,
                'outputsize': 'compact'
            }
            
            logger.info(f"Fetching {symbol} from Alpha Vantage")
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            
            if 'Time Series (Daily)' in data:
                time_series = data['Time Series (Daily)']
                
                for date_str, values in list(time_series.items())[:5]:  # Get last 5 days only for testing
                    record = {
                        'symbol': symbol,
                        'date': datetime.strptime(date_str, '%Y-%m-%d'),
                        'open': float(values['1. open']),
                        'high': float(values['2. high']),
                        'low': float(values['3. low']),
                        'close': float(values['4. close']),
                        'volume': int(values['5. volume']),
                        'data_source': 'alpha_vantage',
                        'extracted_at': datetime.now()
                    }
                    all_data.append(record)
                
                logger.info(f"Fetched {len(time_series)} days for {symbol}")
            else:
                logger.warning(f"No data for {symbol}: {data.get('Note', data.get('Information', 'No data'))}")
        
        if all_data:
            df = pd.DataFrame(all_data)
            logger.info(f"Total Alpha Vantage records: {len(df)}")
            context['ti'].xcom_push(key='alpha_vantage_data', value=df.to_dict('records'))
            return f"Alpha Vantage: {len(df)} records"
        else:
            logger.warning("No Alpha Vantage data fetched")
            return "No Alpha Vantage data"
            
    except Exception as e:
        logger.error(f"Alpha Vantage error: {str(e)}")
        raise

def fetch_finnhub(**context):
    \"\"\"Fetch data from Finnhub\"\"\"
    try:
        logger.info("Starting Finnhub data fetch")
        
        # Get API key - set this in Airflow Variables
        # api_key = Variable.get("FINNHUB_API_KEY")
        api_key = "YOUR_FINNHUB_API_KEY"  # Replace with your actual API key
        
        symbols = ['AAPL', 'MSFT']
        all_data = []
        
        for symbol in symbols:
            # Get quote
            url = "https://finnhub.io/api/v1/quote"
            params = {
                'symbol': symbol,
                'token': api_key
            }
            
            logger.info(f"Fetching {symbol} from Finnhub")
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                record = {
                    'symbol': symbol,
                    'current_price': data.get('c'),
                    'high_price': data.get('h'),
                    'low_price': data.get('l'),
                    'open_price': data.get('o'),
                    'previous_close': data.get('pc'),
                    'timestamp': datetime.fromtimestamp(data.get('t', 0)) if data.get('t') else None,
                    'data_source': 'finnhub',
                    'extracted_at': datetime.now()
                }
                all_data.append(record)
                logger.info(f"Fetched {symbol}: ${record['current_price']}")
            else:
                logger.warning(f"Finnhub API error for {symbol}: {response.status_code}")
        
        if all_data:
            df = pd.DataFrame(all_data)
            logger.info(f"Total Finnhub records: {len(df)}")
            context['ti'].xcom_push(key='finnhub_data', value=df.to_dict('records'))
            return f"Finnhub: {len(df)} records"
        else:
            logger.warning("No Finnhub data fetched")
            return "No Finnhub data"
            
    except Exception as e:
        logger.error(f"Finnhub error: {str(e)}")
        raise

def save_to_postgres(**context):
    \"\"\"Save data to PostgreSQL\"\"\"
    try:
        logger.info("Saving data to PostgreSQL")
        
        # Get data from XCom
        ti = context['ti']
        
        # Initialize database connection
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # Create table for Alpha Vantage data
        cursor.execute(\"\"\"
            CREATE TABLE IF NOT EXISTS alpha_vantage_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20),
                date DATE,
                open DECIMAL(12,4),
                high DECIMAL(12,4),
                low DECIMAL(12,4),
                close DECIMAL(12,4),
                volume BIGINT,
                data_source VARCHAR(50),
                extracted_at TIMESTAMP,
                UNIQUE(symbol, date, data_source)
            )
        \"\"\")
        
        # Create table for Finnhub data
        cursor.execute(\"\"\"
            CREATE TABLE IF NOT EXISTS finnhub_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20),
                current_price DECIMAL(12,4),
                high_price DECIMAL(12,4),
                low_price DECIMAL(12,4),
                open_price DECIMAL(12,4),
                previous_close DECIMAL(12,4),
                timestamp TIMESTAMP,
                data_source VARCHAR(50),
                extracted_at TIMESTAMP,
                UNIQUE(symbol, timestamp, data_source)
            )
        \"\"\")
        
        conn.commit()
        
        # Process Alpha Vantage data
        alpha_data = ti.xcom_pull(task_ids='fetch_alpha_vantage', key='alpha_vantage_data')
        if alpha_data:
            df_alpha = pd.DataFrame(alpha_data)
            logger.info(f"Saving {len(df_alpha)} Alpha Vantage records")
            
            for _, row in df_alpha.iterrows():
                cursor.execute(\"\"\"
                    INSERT INTO alpha_vantage_data 
                    (symbol, date, open, high, low, close, volume, data_source, extracted_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, date, data_source) DO NOTHING
                \"\"\", (
                    row['symbol'], row['date'], row['open'], row['high'], 
                    row['low'], row['close'], row['volume'], 
                    row['data_source'], row['extracted_at']
                ))
        
        # Process Finnhub data
        finnhub_data = ti.xcom_pull(task_ids='fetch_finnhub', key='finnhub_data')
        if finnhub_data:
            df_finnhub = pd.DataFrame(finnhub_data)
            logger.info(f"Saving {len(df_finnhub)} Finnhub records")
            
            for _, row in df_finnhub.iterrows():
                cursor.execute(\"\"\"
                    INSERT INTO finnhub_data 
                    (symbol, current_price, high_price, low_price, open_price, 
                     previous_close, timestamp, data_source, extracted_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, timestamp, data_source) DO NOTHING
                \"\"\", (
                    row['symbol'], row['current_price'], row['high_price'], 
                    row['low_price'], row['open_price'], row['previous_close'],
                    row['timestamp'], row['data_source'], row['extracted_at']
                ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        total_records = (len(alpha_data) if alpha_data else 0) + (len(finnhub_data) if finnhub_data else 0)
        logger.info(f"Saved total {total_records} records to PostgreSQL")
        return f"Saved {total_records} records"
        
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise

def generate_report(**context):
    \"\"\"Generate summary report\"\"\"
    try:
        ti = context['ti']
        
        alpha_result = ti.xcom_pull(task_ids='fetch_alpha_vantage')
        finnhub_result = ti.xcom_pull(task_ids='fetch_finnhub')
        save_result = ti.xcom_pull(task_ids='save_to_postgres')
        
        report = f"""
        Financial Data ETL Report
        =========================
        Generated: {datetime.now()}
        
        Tasks Status:
        - Alpha Vantage: {alpha_result}
        - Finnhub: {finnhub_result}
        - Database Save: {save_result}
        
        All tasks completed successfully!
        """
        
        logger.info(report)
        context['ti'].xcom_push(key='etl_report', value=report)
        return report
        
    except Exception as e:
        logger.error(f"Report error: {str(e)}")
        raise

default_args = {
    'owner': 'financial_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='financial_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for Alpha Vantage and Finnhub financial data',
    schedule_interval='0 9 * * 1-5',  # 9 AM on weekdays
    catchup=False,
    tags=['financial', 'etl', 'alpha_vantage', 'finnhub'],
) as dag:
    
    fetch_alpha_task = PythonOperator(
        task_id='fetch_alpha_vantage',
        python_callable=fetch_alpha_vantage,
        provide_context=True,
    )
    
    fetch_finnhub_task = PythonOperator(
        task_id='fetch_finnhub',
        python_callable=fetch_finnhub,
        provide_context=True,
    )
    
    save_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_to_postgres,
        provide_context=True,
    )
    
    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
    )
    
    # Set dependencies
    [fetch_alpha_task, fetch_finnhub_task] >> save_task >> report_task
