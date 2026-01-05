


import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def get_alpha_vantage_data(**context):
    \"\"\"Fetch data from Alpha Vantage API\"\"\"
    try:
        # Get API key from Airflow Variables (set this in Airflow UI)
        api_key = Variable.get("ALPHA_VANTAGE_API_KEY")
        
        symbols = ['IBM', 'AAPL', 'MSFT', 'GOOGL', 'AMZN']
        function = 'TIME_SERIES_DAILY'
        outputsize = 'compact'  # last 100 days
        
        all_data = []
        
        for symbol in symbols:
            url = f"https://www.alphavantage.co/query"
            params = {
                'function': function,
                'symbol': symbol,
                'apikey': api_key,
                'outputsize': outputsize
            }
            
            response = requests.get(url, params=params)
            data = response.json()
            
            if 'Time Series (Daily)' in data:
                time_series = data['Time Series (Daily)']
                df = pd.DataFrame.from_dict(time_series, orient='index')
                df.columns = ['open', 'high', 'low', 'close', 'volume']
                df.index = pd.to_datetime(df.index)
                df['symbol'] = symbol
                df['extracted_at'] = datetime.now()
                
                # Reset index to have date as column
                df = df.reset_index().rename(columns={'index': 'date'})
                all_data.append(df)
                
                logger.info(f"Fetched {len(df)} days of data for {symbol}")
            else:
                logger.warning(f"No data for {symbol}: {data.get('Note', 'Unknown error')}")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            # Convert columns to numeric
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            combined_df[numeric_cols] = combined_df[numeric_cols].apply(pd.to_numeric)
            
            # Push to XCom
            context['ti'].xcom_push(key='alpha_vantage_data', value=combined_df.to_dict('records'))
            return f"Extracted {len(combined_df)} records from Alpha Vantage"
        else:
            raise Exception("No data extracted from Alpha Vantage")
            
    except Exception as e:
        logger.error(f"Alpha Vantage extraction failed: {str(e)}")
        raise

def get_finnhub_data(**context):
    \"\"\"Fetch data from Finnhub API\"\"\"
    try:
        # Get API key from Airflow Variables
        api_key = Variable.get("FINNHUB_API_KEY")
        
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        
        all_data = []
        
        for symbol in symbols:
            # Get quote
            quote_url = f"https://finnhub.io/api/v1/quote"
            quote_params = {
                'symbol': symbol,
                'token': api_key
            }
            
            quote_response = requests.get(quote_url, params=quote_params)
            quote_data = quote_response.json()
            
            # Get company profile
            profile_url = f"https://finnhub.io/api/v1/stock/profile2"
            profile_params = {
                'symbol': symbol,
                'token': api_key
            }
            
            profile_response = requests.get(profile_url, params=profile_params)
            profile_data = profile_response.json()
            
            # Create record
            record = {
                'symbol': symbol,
                'current_price': quote_data.get('c'),
                'high_price': quote_data.get('h'),
                'low_price': quote_data.get('l'),
                'open_price': quote_data.get('o'),
                'previous_close': quote_data.get('pc'),
                'timestamp': datetime.fromtimestamp(quote_data.get('t', 0)),
                'company_name': profile_data.get('name', ''),
                'exchange': profile_data.get('exchange', ''),
                'market_cap': profile_data.get('marketCapitalization', 0),
                'extracted_at': datetime.now()
            }
            
            all_data.append(record)
            logger.info(f"Fetched data for {symbol}: ${record.get('current_price')}")
        
        if all_data:
            df = pd.DataFrame(all_data)
            context['ti'].xcom_push(key='finnhub_data', value=df.to_dict('records'))
            return f"Extracted {len(df)} records from Finnhub"
        else:
            raise Exception("No data extracted from Finnhub")
            
    except Exception as e:
        logger.error(f"Finnhub extraction failed: {str(e)}")
        raise

def save_to_database(**context):
    \"\"\"Save data to PostgreSQL database\"\"\"
    try:
        # Pull data from both sources
        ti = context['ti']
        
        alpha_data = ti.xcom_pull(task_ids='fetch_alpha_vantage', key='alpha_vantage_data')
        finnhub_data = ti.xcom_pull(task_ids='fetch_finnhub', key='finnhub_data')
        
        # Get database connection from Airflow Connections
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        # Save Alpha Vantage data
        if alpha_data:
            alpha_df = pd.DataFrame(alpha_data)
            
            # Create table if not exists
            create_alpha_table = \"\"\"
            CREATE TABLE IF NOT EXISTS alpha_vantage_stocks (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10),
                date DATE,
                open DECIMAL(12,4),
                high DECIMAL(12,4),
                low DECIMAL(12,4),
                close DECIMAL(12,4),
                volume BIGINT,
                extracted_at TIMESTAMP
            )
            \"\"\"
            
            with engine.connect() as conn:
                conn.execute(create_alpha_table)
                conn.commit()
            
            # Insert data
            alpha_df.to_sql(
                'alpha_vantage_stocks',
                engine,
                if_exists='append',
                index=False
            )
            logger.info(f"Saved {len(alpha_df)} Alpha Vantage records to database")
        
        # Save Finnhub data
        if finnhub_data:
            finnhub_df = pd.DataFrame(finnhub_data)
            
            create_finnhub_table = \"\"\"
            CREATE TABLE IF NOT EXISTS finnhub_stocks (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10),
                current_price DECIMAL(12,4),
                high_price DECIMAL(12,4),
                low_price DECIMAL(12,4),
                open_price DECIMAL(12,4),
                previous_close DECIMAL(12,4),
                timestamp TIMESTAMP,
                company_name VARCHAR(100),
                exchange VARCHAR(10),
                market_cap DECIMAL(20,2),
                extracted_at TIMESTAMP
            )
            \"\"\"
            
            with engine.connect() as conn:
                conn.execute(create_finnhub_table)
                conn.commit()
            
            finnhub_df.to_sql(
                'finnhub_stocks',
                engine,
                if_exists='append',
                index=False
            )
            logger.info(f"Saved {len(finnhub_df)} Finnhub records to database")
        
        return f"Saved {len(alpha_data or []) + len(finnhub_data or [])} total records"
        
    except Exception as e:
        logger.error(f"Database save failed: {str(e)}")
        raise

def generate_report(**context):
    \"\"\"Generate a summary report\"\"\"
    try:
        ti = context['ti']
        
        alpha_data = ti.xcom_pull(task_ids='fetch_alpha_vantage', key='alpha_vantage_data')
        finnhub_data = ti.xcom_pull(task_ids='fetch_finnhub', key='finnhub_data')
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'alpha_vantage_records': len(alpha_data) if alpha_data else 0,
            'finnhub_records': len(finnhub_data) if finnhub_data else 0,
            'status': 'SUCCESS'
        }
        
        logger.info(f"Report: {report}")
        context['ti'].xcom_push(key='etl_report', value=report)
        
        return report
        
    except Exception as e:
        logger.error(f"Report generation failed: {str(e)}")
        raise

# Default DAG arguments
default_args = {
    'owner': 'financial_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Create the DAG
with DAG(
    dag_id='financial_apis_etl',
    default_args=default_args,
    description='ETL pipeline for Alpha Vantage and Finnhub APIs',
    schedule_interval='0 10 * * 1-5',  # 10 AM on weekdays
    catchup=False,
    tags=['alpha_vantage', 'finnhub', 'financial_data'],
) as dag:
    
    # Task 1: Fetch Alpha Vantage data
    fetch_alpha_vantage = PythonOperator(
        task_id='fetch_alpha_vantage',
        python_callable=get_alpha_vantage_data,
        provide_context=True,
    )
    
    # Task 2: Fetch Finnhub data
    fetch_finnhub = PythonOperator(
        task_id='fetch_finnhub',
        python_callable=get_finnhub_data,
        provide_context=True,
    )
    
    # Task 3: Save to database
    save_data = PythonOperator(
        task_id='save_to_database',
        python_callable=save_to_database,
        provide_context=True,
    )
    
    # Task 4: Generate report
    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
    )
    
    # Set task dependencies
    [fetch_alpha_vantage, fetch_finnhub] >> save_data >> generate_report_task
