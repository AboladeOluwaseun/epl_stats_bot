from pathlib import Path
import sys
import pandas as pd
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
from src.processing.base_processor import BaseProcessor
from src.utils.logger import setup_logger
logger = setup_logger(__name__, 'processing.log')

class LeagueProcessor(BaseProcessor):
    """Process leagues data into dim_leagues table."""

    def process_leagues(self) -> int:

        logger.info("Starting leagues processing...")

        # Get raw responses for leagus endpoint
        raw_responses = self.get_raw_api_responses('/leagues')

        if not raw_responses:
               logger.warning("No raw league responses found")
               return 0
        all_leagues = []
        for raw in raw_responses:
               league_data = raw[3].get('response',{})[0]
               league = league_data['league']
               country = league_data['country']
               seasons = league_data['seasons']

               league_record = {
                      'league_id': league.get('id'),
                      'league_name' : league.get('name'),
                      'country' :country.get('name'),
                      'country_code': country.get('code'),
                      'flag_url': country.get('flag'),
                      'logo_url': league.get('logo'),
                      'number_of_seasons': len(seasons)
               }

               all_leagues.append(league_record)
               print(all_leagues)
        df = pd.DataFrame(all_leagues)
        df = df.drop_duplicates(subset=['league_id'], keep='last')



        leagues_count = self.upsert_records(
               table_name = 'leagues',
               records = df.to_dict('records'),
               conflict_columns = ['league_id']
        )

        logger.info(f"Leagues processing completed: {leagues_count} Leagues")
        return leagues_count
    
# if  __name__ == '__main__':
#     process_leagues = LeagueProcessor()
#     result = process_leagues.process_leagues()
#     print(result)