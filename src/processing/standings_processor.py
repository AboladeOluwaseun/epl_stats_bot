from src.processing.base_processor import BaseProcessor
from src.utils.logger import setup_logger
import pandas as pd
import logging
from typing import Dict, List, Any

logger = setup_logger(__name__, 'processing.log')

class StandingsProcessor(BaseProcessor):
    def __init__(self):
        super().__init__()

    def process_standings(self) -> int:
        """
        Fetch raw standings data and process into fact_standings.
        Returns the number of records processed.
        """
        logger.info("Starting standings processing...")
        raw_responses = self.get_raw_api_responses('/standings')

        if not raw_responses:
            logger.warning("No raw standings responses found")
            return 0

        all_standings = []
        # We only care about the latest response for each league/season in real-world,
        # but let's process all unique league/season combinations from raw data.
        
        for raw in raw_responses:
            data = raw[3] # response_data is index 3
            response = data.get('response', [])
            
            for entry in response:
                league = entry.get('league', {})
                league_id = league.get('id')
                season = league.get('season')
                standings_groups = league.get('standings', [])
                
                for group in standings_groups:
                    for row in group:
                        team = row.get('team', {})
                        all_matches = row.get('all', {})
                        
                        standing_record = {
                            'league_id': league_id,
                            'season': season,
                            'rank': row.get('rank'),
                            'team_id': team.get('id'),
                            'points': row.get('points'),
                            'goals_diff': row.get('goalsDiff'),
                            'form': row.get('form'),
                            'played': all_matches.get('played'),
                            'win': all_matches.get('win'),
                            'draw': all_matches.get('draw'),
                            'lose': all_matches.get('lose'),
                            'description': row.get('description'),
                            'updated_at': 'NOW()' # psycopg2 will handle this if passed correctly, but upsert_records uses df.values
                        }
                        all_standings.append(standing_record)

        if not all_standings:
            return 0

        df = pd.DataFrame(all_standings)
        # Drop updated_at from dataframe for manual inclusion in query or use current time
        df['updated_at'] = pd.Timestamp.now()
        
        # Ensure we only keep the latest record for each unique combination
        # Actually, since we process multiple raw responses, we should probably deduplicate
        # by league_id, season, team_id and keep the latest rank/points.
        df = df.drop_duplicates(subset=['league_id', 'season', 'team_id'], keep='last')

        standings_count = self.upsert_records(
            table_name='fact_standings',
            records=df.to_dict('records'),
            conflict_columns=['league_id', 'season', 'team_id']
        )

        logger.info(f"Standings processing completed: {standings_count} entries")
        return standings_count
