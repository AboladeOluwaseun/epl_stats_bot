from pathlib import Path
import sys
import pandas as pd
import json
from typing import List, Dict, Optional

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.processing.base_processor import BaseProcessor
from src.utils.logger import setup_logger
logger = setup_logger(__name__, 'processing.log')

class MatchesProcessor(BaseProcessor):
    """Process fixtures/matches data into matches and match_events tables."""

    def process_matches(self, season: Optional[int] = None) -> Dict[str, int]:
        logger.info(f"Starting matches processing for season {season or 'all'}...")

        # Get raw responses for fixtures endpoint
        raw_responses = self.get_raw_api_responses('/fixtures')

        if not raw_responses:
            logger.warning("No raw fixtures responses found")
            return {'matches': 0, 'events': 0}
        
        all_matches = []
        all_events = []
        all_venues = []

        for raw in raw_responses:
            response_data = raw[3].get('response', [])

            if not response_data:
                logger.warning("Empty response data in raw fixture")
                continue
            
            for fixture_data in response_data:
                try:
                    # Extract nested data
                    fixture = fixture_data.get('fixture', {})
                    league = fixture_data.get('league', {})
                    teams = fixture_data.get('teams', {})
                    goals = fixture_data.get('goals', {})
                    score = fixture_data.get('score', {})

                    # Filter by season if specified
                    if season and league.get('season') != season:
                        continue
                    
                    #Determine winner
                    winner = None
                    if fixture.get('status',{}).get('short','') == 'FT':

                        home_goals = goals.get('home')
                        away_goals = goals.get('away')
                        if home_goals is not None and away_goals is not None:
                            if home_goals > away_goals:
                                winner = 'HOME'
                            elif away_goals > home_goals:
                                winner = 'AWAY'
                            else:
                                winner = 'DRAW'
                        
                    # Build match record
                    match_record = {
                        'fixture_id': fixture.get('id'),
                        'league_id': league.get('id'),
                        'season': league.get('season'),
                        'home_team_id': teams.get('home', {}).get('id'),
                        'away_team_id': teams.get('away', {}).get('id'),
                        'venue_id': fixture.get('venue', {}).get('id'),
                        'match_date': fixture.get('date'),
                        'round': league.get('round'),
                        'status': fixture.get('status', {}).get('short'),
                        'status_long': fixture.get('status', {}).get('long'),
                        'referee': fixture.get('referee'),
                        'timezone': fixture.get('timezone'),
                        'home_goals': goals.get('home'),
                        'away_goals': goals.get('away'),
                        'halftime_home_goals': score.get('halftime', {}).get('home'),
                        'halftime_away_goals': score.get('halftime', {}).get('away'),
                        'fulltime_home_goals': score.get('fulltime', {}).get('home'),
                        'fulltime_away_goals': score.get('fulltime', {}).get('away'),
                        'extratime_home_goals': score.get('extratime', {}).get('home'),
                        'extratime_away_goals': score.get('extratime', {}).get('away'),
                        'penalty_home_goals': score.get('penalty', {}).get('home'),
                        'penalty_away_goals': score.get('penalty', {}).get('away'),
                        'winner': winner,
                    }

                    all_matches.append(match_record)

                    # Extract venue
                    venue_data = fixture.get('venue', {})
                    if venue_data.get('id'):
                        all_venues.append({
                            'venue_id': venue_data.get('id'),
                            'venue_name': venue_data.get('name'),
                            'city': venue_data.get('city')
                        })
                
                except Exception as e:
                    logger.error(f"Error processing fixture: {e}", exc_info=True)
                    continue

        if not all_matches:
            logger.warning("No matches extracted from raw data")
            return {'matches': 0, 'events': 0}
        
        # Upsert Venues First
        if all_venues:
            df_venues = pd.DataFrame(all_venues)
            df_venues = df_venues.drop_duplicates(subset=['venue_id'], keep='last')
            
            venue_count = self.upsert_records(
                table_name='dim_venues',
                records=df_venues.to_dict('records'),
                conflict_columns=['venue_id']
            )
            logger.info(f"Upserted {venue_count} venues from matches")

        df_matches = pd.DataFrame(all_matches)
        df_matches = df_matches.drop_duplicates(subset=['fixture_id'], keep='last')
        logger.info(f"Extracted {len(df_matches)} unique matches")

        # Upsert matches
        matches_count = self.upsert_records(
            table_name='matches',
            records=df_matches.to_dict('records'),
            conflict_columns=['fixture_id']
        )

        logger.info(f"Matches processing completed: {matches_count} matches upserted")
        
        return {'matches': matches_count, 'events': 0}

                


