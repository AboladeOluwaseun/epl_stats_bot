from pathlib import Path
import sys
import pandas as pd
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
from src.processing.base_processor import BaseProcessor
from src.utils.logger import setup_logger

logger = setup_logger(__name__, 'processing.log')


class SeasonsProcessor(BaseProcessor):
    def process_seasons(self) -> int:
        logger.info("Starting seasons processing...")

        raw_response = self.get_raw_api_responses('/leagues')

        if not raw_response:
            logger.warning("No raw league responses found")
            return 0
        
        all_seasons = []

        for raw in raw_response:
            league_data = raw[3].get('response',{})[0]

            seasons = league_data.get('seasons',[])
            league_id = league_data.get('league', {})['id']
            for season in seasons:
                season_record ={
                    'season_year': season.get('year'),
                    'season_name': f"{season.get('start')[:4]}-{season.get('end')[:4]}",
                    'league_id' : league_id,
                    'start_date': season.get('start'),
                    'end_date':season.get('end'),
                    'is_current': season.get('current')
                }

                all_seasons.append(season_record)
        
        df = pd.DataFrame(all_seasons)
        df = df.drop_duplicates(subset=['season_year', 'league_id'], keep='last')

        logger.info(f"Extracted {len(df)} unique seasons")

        seasons_count = self.upsert_records(
            table_name='dim_seasons',
            records=df.to_dict('records'),
            conflict_columns=['season_name']
            )
        logger.info(f"Seasons processing completed: {seasons_count} seasons")
        return seasons_count

# if __name__ == '__main__':
#     processor = SeasonsProcessor()
#     result = processor.process_seasons()
#     print(f"Processed {result} seasons")-+9