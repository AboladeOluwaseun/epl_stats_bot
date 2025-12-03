import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from typing import List, Dict, Optional, Any
from pathlib import Path
import sys
import json

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
from src.utils.configs import config
from src.utils.logger import setup_logger
from contextlib import contextmanager

from src.ingestion.api_client import FootballAPIClient

logger = setup_logger(__name__, 'database.log')

class PostgresHandler:
    """Handler for PostgreSQL database operations."""
    
    def __init__(self):
        self.connection_params = {
            'host': config.POSTGRES_HOST,
            'port': config.POSTGRES_PORT,
            'database': config.POSTGRES_DB,
            'user': config.POSTGRES_USER,
            'password': config.POSTGRES_PASSWORD
        }

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def execute_query(self, query, params:Optional[tuple]=None, fetch=True):
        # query = '''

        #     '''
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall() 
                return None

    def insert_raw_responses(self, endpoint, request_params, response_data):
        
        query = """
        INSERT INTO raw_api_responses (endpoint, request_params, response_data)
        VALUES (%s, %s, %s)
        RETURNING response_id
    """
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (endpoint, json.dumps(request_params),
                    json.dumps(response_data)))
                return cur.fetchone()[0]

