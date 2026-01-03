from pathlib import Path
import sys
import pandas as pd
import json

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.processing.base_processor import BaseProcessor
from src.utils.logger import setup_logger

logger = setup_logger(__name__, 'processing.log')

class TeamsProcessor(BaseProcessor):
    def process_teams_and_venues(self, seasons=None) -> dict:

        logger.info("Starting teams and venues processing...")

        # Get ALL raw responses for teams endpoint
        # Modified query to get all seasons if none specified
        if seasons:
            # Get specific seasons
            query = """
                SELECT 
                    response_id,
                    endpoint,
                    request_params,
                    response_data,
                    fetched_at
                FROM raw_api_responses
                WHERE endpoint = %s
                AND (request_params->>'season')::int = ANY(%s)
                ORDER BY fetched_at DESC
            """
            raw_responses = self.db_handler.execute_query(query, ('/teams', seasons))
            logger.info(f'${raw_responses}')
        else:
            # Get all seasons
            raw_responses = self.get_raw_api_responses('/teams')

        if not raw_responses:
            logger.warning("No raw teams responses found")
            return {'teams': 0, 'venues': 0}
        
        logger.info(f"Processing {len(raw_responses)} raw team responses...")

        all_teams = []
        all_venues = []
        seasons_processed = set()

        for raw in raw_responses:
            # Access response_data (4th column in your structure)
            response_data = raw[3]
            request_params = raw[2]
            season = request_params.get('season')

            seasons_processed.add(season)
        
            # Get the teams array from response
            teams_data = response_data.get('response', [])

            logger.info(f"Processing {len(teams_data)} teams...")

            for item in teams_data:
                # Extract team data
                team = item.get('team', {})
                venue = item.get('venue', {})

                # Process team record
                team_record = {
                    'team_id': team.get('id'),
                    'team_name': team.get('name'),
                    'short_name': team.get('code'),  # 3-letter code (e.g., MUN)
                    'team_code': team.get('code'),
                    'country': team.get('country'),
                    'founded_year': team.get('founded'),
                    'is_national': team.get('national', False),
                    'logo_url': team.get('logo'),
                    'venue_id': venue.get('id')  # Foreign key to venues
                }
                all_teams.append(team_record)

                # Process venue record (if exists)
                if venue.get('id'):
                    venue_record = {
                        'venue_id': venue.get('id'),
                        'venue_name': venue.get('name'),
                        'address': venue.get('address'),
                        'city': venue.get('city'),
                        'capacity': venue.get('capacity'),
                        'surface': venue.get('surface'),
                        'image_url': venue.get('image')
                    }
                    all_venues.append(venue_record)
        
        logger.info(f"Extracted data from seasons: {sorted(seasons_processed)}")
        logger.info(f"Total teams before dedup: {len(all_teams)}")
        logger.info(f"Total venues before dedup: {len(all_venues)}")

        # Remove duplicates
        df_teams = pd.DataFrame(all_teams)
        df_teams = df_teams.drop_duplicates(subset=['team_id'], keep='last')

        df_venues = pd.DataFrame(all_venues)
        df_venues = df_venues.drop_duplicates(subset=['venue_id'], keep='last')

        logger.info(f"After deduplication: {len(df_teams)} unique teams, {len(df_venues)} unique venues")

        # Upsert venues first (teams reference venues)
        venues_count = 0
        if not df_venues.empty:
            venues_count = self.upsert_records(
                table_name='dim_venues',
                records=df_venues.to_dict('records'),
                conflict_columns=['venue_id']
            )
            logger.info(f"Processed {venues_count} venues")
        
        # Upsert teams
        teams_count = 0
        if not df_teams.empty:
            teams_count = self.upsert_records(
                table_name='dim_teams',
                records=df_teams.to_dict('records'),
                conflict_columns=['team_id']
            )
            logger.info(f"Processed {teams_count} teams")

        logger.info(f"Teams and venues processing completed!")
        
        return {
            'teams': teams_count,
            'venues': venues_count,
            'seasons_processed': sorted(seasons_processed)
        } 

        

# if __name__ == '__main__':
#     processor = TeamsProcessor()
#     result = processor.process_teams_and_venues()
#     print(f"Processed: {json.dumps(result, indent=4)}")
