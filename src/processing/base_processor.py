from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.storage.postgres_handler import PostgresHandler
from src.utils.logger import setup_logger

logger = setup_logger(__name__, 'processing.log')
class BaseProcessor:
    def __init__(self):
        self.db_handler=PostgresHandler()
    def get_raw_api_responses(self, endpoint):
        query = """
            SELECT 
                response_id,
                endpoint,
                request_params,
                response_data,
                fetched_at
            FROM raw_api_responses
            WHERE endpoint = '/leagues'
            ORDER BY fetched_at DESC
        """
        results = self.db_handler.execute_query(query)
        logger.info(f"Fetched  raw responses for endpoint: {endpoint}")
        return results

    def upsert_records(self):
        with self.db_handler.get_connection() as conn:
            with conn.cursor() as cur:
                

# if __name__ == '__main__':
#     processor= BaseProcessor()
#     results= processor.get_raw_api_responses('leagues')
#     print(results)