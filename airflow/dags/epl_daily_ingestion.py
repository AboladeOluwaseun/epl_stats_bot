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
from src.processing.pipeline import ProcessingPipeline
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
    'epl_complete_pipeline',
    default_args=default_args,
    description='Complete EPL data pipeline: Ingestion → Processing → Aggregation',
    start_date= datetime(2025, 12, 18),
    schedule= '0 6 * * *',
    tags=['epl', 'ingestion', 'daily'],
    catchup=False
)

def fetch_league():
    fetch_and_store_league_data = Datafetcher()
    logger.info("Task: Fetching league...")
    league = fetch_and_store_league_data.fetch_and_store_league()
    return league

def fetch_teams():
    fetch_and_store_teams= Datafetcher()
    logger.info('Task: Fetching teams...')
    teams = fetch_and_store_teams.fetch_and_store_all_epl_teams_historical()
    return teams

def processing_pipeline():
    process_data = ProcessingPipeline()
    logger.info('Task:Running processing pipeline')
    results = process_data.run_full_processing()
    return results


fetch_and_store_league_task = PythonOperator(
    task_id='fetch_league',
    python_callable=fetch_league,
    provide_context=True,
    dag=dag,
)

fetch_and_store_teams_task = PythonOperator(
    task_id='fetch_and_store_teams',
    python_callable=fetch_teams,
    provide_context=True,
    dag=dag,
)

process_data_task = PythonOperator(
     task_id = 'process_data',
     python_callable = processing_pipeline,
     provide_context = True,
     dag = dag
)

[fetch_and_store_league_task, fetch_and_store_teams_task ]>> process_data_task