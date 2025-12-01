from utils.logger import setup_logger
from utils.configs import config

from ingestion.api_client import FootballAPIClient
from storage.postgres_handler import PostgresHandler

logger = setup_logger(__name__, "ingestion.log")

class Datafetcher:
    def __init__(self):
        self.api_client = FootballAPIClient()
        self.db_handler = PostgresHandler()