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
    def get_stored_league(self):
        query = ''' SELECT * FROM raw_api_responses'''
        result = self.db_handler.execute_query(query)
        return result
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
        
    
# if __name__ == '__main__':
#     fetcher = Datafetcher()
#     result = fetcher.fetch_and_store_league()
#     print(result)

    