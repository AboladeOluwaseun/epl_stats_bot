from pathlib import Path
import sys
import json
import pandas as pd
from typing import List, Dict, Any, Optional
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
            WHERE endpoint = %s
            ORDER BY fetched_at DESC
        """
        results = self.db_handler.execute_query(query, (endpoint,))
        logger.info(f"Fetched  raw responses for endpoint: {endpoint}")
        return results

    def upsert_records(self, table_name, records:List[Dict],conflict_columns:List[str]):
        if not records:
            logger.warning(f"No records to upsert into {table_name}")
            return 0
        df = pd.DataFrame(
            records
        )
       
        # Build upsert query
        columns = list(df.columns)
        placeholders = ', '.join(['%s'] * len(columns))
        conflict_cols = ', '.join(conflict_columns)
        update_cols = ', '.join([
            f"{col} = EXCLUDED.{col}" 
            for col in columns 
            if col not in conflict_columns
        ])

        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES %s
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {update_cols}
        """
        with self.db_handler.get_connection() as conn:
            with conn.cursor() as cur:
                from psycopg2.extras import execute_values
                execute_values(
                    cur,
                    query,
                    df.values,
                    template=None,
                    page_size=100
                )
        logger.info(f"Upserted {len(records)} records into {table_name}")
        return len(records)
                

# if __name__ == '__main__':
    # processor= BaseProcessor()
    # results= processor.get_raw_api_responses('/leagues')
    # response_data = results
#     # df= processor.upsert_records('league', results, ['i','i'])
    # print(json.dumps(results, indent=4, default=str))
#     # df= pd.DataFrame(results)
#     # df.to_csv('epl', index=False)
#     # df.show()
    # print(df.head())