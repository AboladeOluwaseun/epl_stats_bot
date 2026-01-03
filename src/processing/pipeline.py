from pathlib import Path
import sys
import pandas as pd
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
from src.processing.league_processor import LeagueProcessor
from src.processing.seasons_processor import SeasonsProcessor
from src.processing.teams_processor import TeamsProcessor
from src.processing.matches_processor import MatchesProcessor
from src.processing.players_processor import PlayersProcessor
from src.processing.standings_processor import StandingsProcessor
from src.utils.logger import setup_logger


logger = setup_logger(__name__, 'processing.log')

class ProcessingPipeline:

    def __init__(self):
        self.league_processor = LeagueProcessor()
        self.seasons_processor = SeasonsProcessor()
        self.teams_processor = TeamsProcessor()
        self.matches_processor = MatchesProcessor()
        self.players_processor = PlayersProcessor()
        self.standings_processor = StandingsProcessor()

    def run_full_processing(self):
        logger.info("=" * 60)
        logger.info("Starting full processing pipeline...")
        logger.info("=" * 60)

        results = {
            'success': True,
            'leagues_count':0,
            'seasons_count':0,
            'teams_count': 0,
            'matches_count': 0,
            'standings_count': 0,
            'players_stats_count': 0,
            'player_profiles_count': 0,
            'errors': []
        }

        try:
            results['leagues_count']= self.league_processor.process_leagues()
            results['seasons_count']= self.seasons_processor.process_seasons()
            results['teams_count']= self.teams_processor.process_teams_and_venues()
            results['matches_count'] = self.matches_processor.process_matches()
            results['standings_count'] = self.standings_processor.process_standings()
            
            # Process player stats for completed matches
            player_results = self.players_processor.process_player_stats()
            results['players_stats_count'] = player_results.get('stats_entries', 0)
            
            # Process player profiles (from /players/profiles endpoint)
            profile_results = self.players_processor.process_player_profiles()
            results['player_profiles_count'] = profile_results.get('profiles_processed', 0)

            logger.info("Processing pipeline completed successfully!")
            logger.info(f"Leagues: {results['leagues_count']}")
            logger.info(f"Seasons: {results['seasons_count']}")
            logger.info(f"Standings: {results['standings_count']}")
            logger.info(f"Player Stats: {results['players_stats_count']}")
            logger.info(f"Player Profiles: {results['player_profiles_count']}")
            
            
        except Exception as e:
            logger.error(f"Error in processing pipeline: {e}", exc_info=True)
            results['success'] = False
            results['errors'].append(str(e))
            
        return results
    
