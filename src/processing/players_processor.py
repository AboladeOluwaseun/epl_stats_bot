from pathlib import Path
import sys
import pandas as pd
import json
import time
from typing import List, Dict, Optional

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.processing.base_processor import BaseProcessor
from src.utils.logger import setup_logger

logger = setup_logger(__name__, 'processing.log')

class PlayersProcessor(BaseProcessor):
    """Process detailed player statistics for fixtures."""
    
    def __init__(self):
        super().__init__()
        # Removed direct API client usage

    def process_player_stats(self) -> Dict[str, int]:
        """
        Process player stats from raw_api_responses table.
        """
        logger.info("Starting player stats processing...")
        
        # 1. Get raw responses from DB
        raw_responses = self.get_raw_api_responses('/fixtures/players')
        
        if not raw_responses:
            logger.info("No raw player stats responses found.")
            return {'players_processed': 0, 'stats_entries': 0}
            
        logger.info(f"Found {len(raw_responses)} raw player stats responses. Processing...")
        
        total_players_upserted = 0
        total_stats_upserted = 0
        
        for raw in raw_responses:
            try:
                # raw structure: (id, endpoint, params, response_data, fetched_at)
                response_data = raw[3]
                request_params = raw[2]
                fixture_id = request_params.get('fixture')
                
                if not response_data or not response_data.get('response'):
                    continue
                
                fixture_players = []
                fixture_stats = []
                
                # 3. Parse Response
                for team_data in response_data['response']:
                    team_id = team_data['team']['id']
                    
                    for player_entry in team_data['players']:
                        player_info = player_entry['player']
                        stats_info = player_entry['statistics'][0]
                        
                        # Prepare Dimension Record
                        p_record = {
                            'player_id': player_info['id'],
                            'player_name': player_info['name'],
                            'photo_url': player_info['photo']
                        }
                        fixture_players.append(p_record)
                        
                        # Prepare Fact Record
                        s_record = {
                            'fixture_id': fixture_id,
                            'player_id': player_info['id'],
                            'team_id': team_id,
                            'minutes_played': stats_info['games']['minutes'],
                            'rating': stats_info['games']['rating'],
                            'captain': stats_info['games']['captain'],
                            'substitute': stats_info['games']['substitute'],
                            'offside': stats_info['offsides'],
                            'shots_total': stats_info['shots']['total'],
                            'shots_on_target': stats_info['shots']['on'],
                            'goals_total': stats_info['goals']['total'],
                            'goals_conceded': stats_info['goals']['conceded'],
                            'assists': stats_info['goals']['assists'],
                            'saves': stats_info['goals']['saves'],
                            'passes_total': stats_info['passes']['total'],
                            'passes_key': stats_info['passes']['key'],
                            'passes_accuracy': stats_info['passes']['accuracy'],
                            'tackles_total': stats_info['tackles']['total'],
                            'blocks': stats_info['tackles']['blocks'],
                            'interceptions': stats_info['tackles']['interceptions'],
                            'duels_total': stats_info['duels']['total'],
                            'duels_won': stats_info['duels']['won'],
                            'dribbles_attempts': stats_info['dribbles']['attempts'],
                            'dribbles_success': stats_info['dribbles']['success'],
                            'dribbles_past': stats_info['dribbles']['past'],
                            'fouls_drawn': stats_info['fouls']['drawn'],
                            'fouls_committed': stats_info['fouls']['committed'],
                            'yellow_cards': stats_info['cards']['yellow'],
                            'red_cards': stats_info['cards']['red'],
                            'penalty_won': stats_info['penalty']['won'],
                            'penalty_commited': stats_info['penalty']['commited'],
                            'penalty_scored': stats_info['penalty']['scored'],
                            'penalty_missed': stats_info['penalty']['missed'],
                            'penalty_saved': stats_info['penalty']['saved'],
                        }
                        fixture_stats.append(s_record)

                # 4. Upsert Data
                if fixture_players:
                    df_p = pd.DataFrame(fixture_players).drop_duplicates(subset=['player_id'])
                    self.upsert_records(
                        'dim_players', 
                        df_p.to_dict('records'), 
                        ['player_id']
                    )
                    total_players_upserted += len(df_p)
                
                if fixture_stats:
                    df_s = pd.DataFrame(fixture_stats)
                    
                    # Robust Int Casting: prevent Pandas from sending floats (due to NaNs) to Postgres Integer columns
                    int_cols = [
                        'minutes_played', 'offside', 'shots_total', 'shots_on_target', 
                        'goals_total', 'goals_conceded', 'assists', 'saves', 
                        'passes_total', 'passes_key', 'tackles_total', 'blocks', 
                        'interceptions', 'duels_total', 'duels_won', 'dribbles_attempts', 
                        'dribbles_success', 'dribbles_past', 'fouls_drawn', 'fouls_committed', 
                        'yellow_cards', 'red_cards', 'penalty_won', 'penalty_commited', 
                        'penalty_scored', 'penalty_missed', 'penalty_saved'
                    ]
                    
                    # Also include the BIGINT IDs just to be safe with type consistency
                    id_cols = ['fixture_id', 'player_id', 'team_id']
                    
                    for col in int_cols + id_cols:
                        if col in df_s.columns:
                            df_s[col] = pd.to_numeric(df_s[col], errors='coerce').fillna(0).astype('int64')

                    df_s['rating'] = pd.to_numeric(df_s['rating'], errors='coerce')
                    
                    self.upsert_records(
                        'fact_player_stats',
                        df_s.to_dict('records'),
                        ['fixture_id', 'player_id']
                    )
                    total_stats_upserted += len(df_s)
                    
            except Exception as e:
                logger.error(f"Error processing stats for fixture {fixture_id}: {e}", exc_info=True)
                continue
                
        logger.info(f"Player stats processing complete. Upserted {total_stats_upserted} stat entries.")
        return {
            'players_processed': total_players_upserted,
            'stats_entries': total_stats_upserted
        }

    def process_player_profiles(self) -> Dict[str, int]:
        """
        Process player profiles from /players raw responses (EPL targeted).
        Populates dim_players with full profile data (name, age, nationality, position, etc.)
        """
        logger.info("Starting player profiles processing (EPL only)...")
        
        # Use /players instead of /players/profiles for EPL-only data
        raw_responses = self.get_raw_api_responses('/players')
        
        if not raw_responses:
            logger.info("No raw player responses found for EPL.")
            return {'profiles_processed': 0}
            
        logger.info(f"Found {len(raw_responses)} raw response batches. Processing...")
        
        all_players = []
        
        for raw in raw_responses:
            try:
                response_data = raw[3]
                
                if not response_data or not response_data.get('response'):
                    continue
                
                for item in response_data['response']:
                    player = item.get('player', {})
                    birth = player.get('birth', {})
                    
                    p_record = {
                        'player_id': player.get('id'),
                        'player_name': player.get('name'),
                        'firstname': player.get('firstname'),
                        'lastname': player.get('lastname'),
                        'age': player.get('age'),
                        'birth_date': birth.get('date'),
                        'birth_place': birth.get('place'),
                        'birth_country': birth.get('country'),
                        'nationality': player.get('nationality'),
                        'height': player.get('height'),
                        'weight': player.get('weight'),
                        'number': player.get('number'),
                        'position': player.get('position'),
                        'photo_url': player.get('photo')
                    }
                    all_players.append(p_record)
                    
            except Exception as e:
                logger.error(f"Error parsing profile response: {e}", exc_info=True)
                continue
        
        if all_players:
            df = pd.DataFrame(all_players).drop_duplicates(subset=['player_id'],)
            
            # Ensure integer columns don't have NaNs which cast them to float
            for col in ['age', 'number']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
            self.upsert_records('dim_players', df.to_dict('records'), ['player_id'])
            logger.info(f"Upserted {len(df)} player profiles.")
            return {'profiles_processed': len(df)}
        
        return {'profiles_processed': 0}
