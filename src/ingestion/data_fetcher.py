from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.configs import config
from src.utils.logger import setup_logger
from src.ingestion.api_client import FootballAPIClient
from src.storage.postgres_handler import PostgresHandler

logger = setup_logger(__name__, "ingestion.log")

class Datafetcher:
    def __init__(self):
        self.api_client = FootballAPIClient()
        self.db_handler = PostgresHandler()
    # def get_stored_league(self):
    #     query = ''' SELECT * FROM raw_api_responses'''
    #     result = self.db_handler.execute_query(query)
    #     return result
    def fetch_and_store_league(self):
        logger.info('fetching epl league data')

        league = self.api_client.get_league()

        if not league:
            logger.warning('No league data fetched')
            return False
        # logger.info(f'Fetched standings:{league[3]}')

        self.db_handler.insert_raw_responses(
            '/leagues',
            {'id':39},
            league
        ) 
       
        return league
    
    def fetch_and_store_teams(self):
        logger.info('Fetching EPL teams data...')
        teams = self.api_client.get_teams(league_id=39, season=2023)

        if not teams:
            logger.warning('No teams data fetched')
            return False
        
        logger.info(f"Fetched {teams.get('results', 0)} teams")

        # Store raw response
        self.db_handler.insert_raw_responses(
            '/teams',
            {'league': 39, 'season': 2023},
            teams
        )
        return True
        
    def fetch_and_store_teams_multi_season(self, seasons=None):

        if seasons is None:
            # Default: last 5 seasons
            current_year = 2024
            seasons = list(range(current_year - 4, current_year + 1))
        logger.info(f'Fetching EPL teams for multiple seasons: {seasons}')

        results = {
            'seasons_fetched': 0,
            'total_teams': 0,
            'failed_seasons': []
        }

        for season in seasons:
            logger.info(f'Fetching teams for season {season}...')
            try:
                teams = self.api_client.get_teams(league_id=39, season=season)
                
                if not teams or teams.get('results', 0) == 0:
                    logger.warning(f'No teams data for season {season}')
                    results['failed_seasons'].append(season)
                    continue
                
                teams_count = teams.get('results', 0)
                logger.info(f"Season {season}: Fetched {teams_count} teams")
                
                # Store raw response
                self.db_handler.insert_raw_responses(
                    '/teams',
                    {'league': 39, 'season': season},
                    teams
                )
                results['seasons_fetched'] += 1
                results['total_teams'] += teams_count
            
            except Exception as e:
                logger.error(f"Error fetching teams for season {season}: {e}")
                results['failed_seasons'].append(season)
                continue

            logger.info(f"Multi-season fetch complete: {results}")
        return results
        
    def fetch_and_store_all_epl_teams_historical(self):

        logger.info('Fetching ALL historical EPL teams (2010-2024)...')
        # EPL seasons from 2010 to 2024
        seasons = list(range(2010, 2024))  # 2010, 2011, ..., 2024
        return self.fetch_and_store_teams_multi_season(seasons=seasons)
    

# if __name__ == '__main__':
#     fetcher = Datafetcher()
#     result = fetcher.fetch_and_store_teams()
#     print(result)

    