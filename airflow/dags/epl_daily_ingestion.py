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
    start_date= datetime(2026, 1, 1),
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

def fetch_fixtures():
    fetch_and_store_fixtures = Datafetcher()
    logger.info('Task:Fetching fixtures...')
    fixtures = fetch_and_store_fixtures.fetch_and_store_all_epl_fixtures_historical()
    return fixtures

def fetch_player_stats():
    fetcher = Datafetcher()
    logger.info('Task: Fetching player stats...')
    # Fetch stats for up to 30 matches
    count = fetcher.fetch_and_store_player_stats(limit=30)
    return count

# def fetch_player_profiles():
#     fetcher = Datafetcher()
#     logger.info('Task: Fetching EPL player profiles/stats for multiple seasons...')
#     # Fetch EPL players for the last 3 seasons (default)
#     seasons = list(range(2021, 2025)) 
#     results = fetcher.fetch_and_store_player_profiles_multi_season(seasons)
#     return results

def fetch_player_profiles():
    fetcher = Datafetcher()
    logger.info('Task: Syncing current season EPL player rosters (2024)...')
    # Fetch ONLY current season to stay within credits but ensure bench players exist
    results = fetcher.fetch_and_store_player_profiles(season=2024)
    return results

def processing_pipeline():
    process_data = ProcessingPipeline()
    logger.info('Task:Running processing pipeline')
    results = process_data.run_full_processing()
    return results


def repair_player_profiles():
    fetcher = Datafetcher()
    logger.info('Task: Repairing skeleton player profiles...')
    count = fetcher.fetch_and_store_missing_player_profiles(limit=80)
    return count

def fetch_standings():
    fetcher = Datafetcher()
    logger.info('Task: Fetching (storing) league standings...')
    results = fetcher.fetch_and_store_standings(season=2024)
    return results

repair_player_profiles_task = PythonOperator(
    task_id='repair_player_profiles',
    python_callable=repair_player_profiles,
    provide_context=True,
    dag=dag,
)

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

fetch_and_store_fixtures_task = PythonOperator(
    task_id = 'fetch_and_store_fixtures',
    python_callable = fetch_fixtures,
    provide_context= True,
    dag=dag
)

# Phase 1: Process initial data (Leagues, Teams, Matches)
process_data_initial_task = PythonOperator(
     task_id = 'process_data_initial',
     python_callable = processing_pipeline,
     provide_context = True,
     dag = dag
)

# Phase 2: Fetch detailed stats for matches known in Phase 1
fetch_player_stats_task = PythonOperator(
    task_id = 'fetch_player_stats',
    python_callable = fetch_player_stats,
    provide_context = True,
    dag = dag
)

# Phase 2b: Fetch player profiles (for rich dim_players data)
fetch_player_profiles_task = PythonOperator(
    task_id = 'fetch_player_profiles',
    python_callable = fetch_player_profiles,
    provide_context = True,
    dag = dag
)

# Phase 3: Process the newly fetched player stats and profiles
process_data_final_task = PythonOperator(
     task_id = 'process_data_final',
     python_callable = processing_pipeline,
     provide_context = True,
     dag = dag
)



fetch_standings_task = PythonOperator(
    task_id='fetch_standings',
    python_callable=fetch_standings,
    provide_context=True,
    dag=dag,
)




# ============================================================================
# DAG Dependencies
# ============================================================================

# Phase 1: Ingest Basic Data & Initial Processing
[fetch_and_store_league_task, fetch_and_store_teams_task, fetch_and_store_fixtures_task, fetch_standings_task] >> process_data_initial_task

# Phase 2: Targeted Detail Enrichment (Sync rosters -> Fetch recent match stats -> Repair bios)
# 1. Syncing rosters (2024) ensures all players exist in DB.
# 2. Fetching match stats finds who played.
# 3. Repair fills detailed bios for those who actually featured.
process_data_initial_task >> fetch_player_stats_task >> repair_player_profiles_task >> process_data_final_task