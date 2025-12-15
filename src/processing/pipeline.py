from pathlib import Path
import sys
import pandas as pd
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
from src.processing.league_processor import LeagueProcessor
from src.processing.seasons_processor import SeasonsProcessor
from src.utils.logger import setup_logger


logger = setup_logger(__name__, 'processing.log')

class ProcessingPipeline:

    def __init__(self):
        self.league_processor = LeagueProcessor()
        self.seasons_processor = SeasonsProcessor()

    def run_full_processing(self):
        logger.info("=" * 60)
        logger.info("Starting full processing pipeline...")
        logger.info("=" * 60)

        results = {
            'success': True,
            'leagues_count':0,
            'seasons_count':0
        # 'teams_count': 0,
        # 'matches_count': 0,
        # 'players_count': 0,
        # 'goals_count': 0,
        # 'views_refreshed': False,
        # 'errors': []
        }

        try:
            results['leagues_count']= self.league_processor.process_leagues()
            results['seasons_count']= self.seasons_processor.process_seasons()

            logger.info("Processing pipeline completed successfully!")
            logger.info(f"Leagues: {results['leagues_count']}")
            logger.info(f"Seasons: {results['seasons_count']}")
        except Exception as e:
            logger.error(f"Error in processing pipeline: {e}", exc_info=True)
            results['success'] = False
            results['errors'].append(str(e))
            
        return results
    
