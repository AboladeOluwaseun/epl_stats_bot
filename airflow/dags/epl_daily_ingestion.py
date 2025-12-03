from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path
import sys
sys.path.insert(0, '/opt/airflow')
from src.utils.logger import setup_logger
from src.ingestion.data_fetcher import Datafetcher

logger = setup_logger(__name__, )
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    'epl_daily_ingestion',
    default_args=default_args,
    description='Daily EPL data ingestion pipeline',
    start_date= datetime(2025, 12, 2),
    schedule= '0 6 * * *',
    tags=['epl', 'ingestion', 'daily']
)

def fetch_league():
    fetch_and_store_league_data = Datafetcher()
    logger.info("Task: Fetching league...")
    league = fetch_and_store_league_data.fetch_and_store_league()
    return league
    


fetch_league_task = PythonOperator(
    task_id='fetch_league',
    python_callable=fetch_league,
    provide_context=True,
    dag=dag,
)