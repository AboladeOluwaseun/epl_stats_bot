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
from src.processing.league_processor import LeagueProcessor
from src.processing.seasons_processor import SeasonsProcessor
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
    start_date= datetime(2025, 12, 13),
    schedule= '0 6 * * *',
    tags=['epl', 'ingestion', 'daily'],
    catchup=False
)

def fetch_league():
    fetch_and_store_league_data = Datafetcher()
    logger.info("Task: Fetching league...")
    league = fetch_and_store_league_data.fetch_and_store_league()
    return league

def processing_pipeline():
        process_data = ProcessingPipeline()
        results = process_data.run_full_processing()
        return results
def process_league():
    process_league_data = LeagueProcessor()
    logger.info('Task: Processing league data')
    league_count = process_league_data.process_leagues()
    return league_count

def process_seasons():
    process_seasons_data = SeasonsProcessor()
    logger.info('Task: Processing seasons data')
    seasons_count = process_seasons_data.process_seasons()
    return seasons_count

fetch_and_store_league_task = PythonOperator(
    task_id='fetch_league',
    python_callable=fetch_league,
    provide_context=True,
    dag=dag,
)

# process_league_task = PythonOperator(
#     task_id='process_league',
#     python_callable = process_league,
#     provide_context = True,
#     dag = dag
# )

# process_seasons_task = PythonOperator(
#     task_id='process_seasons',
#     python_callable = process_seasons,
#     provide_context = True,
#     dag = dag
# )

process_data_task = PythonOperator(
     task_id = 'process_data',
     python_callable = processing_pipeline,
     provide_context = True,
     dag = dag
)

fetch_and_store_league_task >> process_data_task