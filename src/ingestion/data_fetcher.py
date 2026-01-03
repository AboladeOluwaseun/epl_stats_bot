from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
import time
from src.utils.configs import config
from src.utils.logger import setup_logger
from src.ingestion.api_client import FootballAPIClient
from src.storage.postgres_handler import PostgresHandler

logger = setup_logger(__name__, "ingestion.log")

class Datafetcher:
    def __init__(self):
        self.api_client = FootballAPIClient()
        self.db_handler = PostgresHandler()
    # def get_stored_league(self):
    #     query = ''' SELECT * FROM raw_api_responses'''
    #     result = self.db_handler.execute_query(query)
    #     return result
    def fetch_and_store_league(self):
        logger.info('fetching epl league data')

        league = self.api_client.get_league()

        if not league:
            logger.warning('No league data fetched')
            return False
        # logger.info(f'Fetched standings:{league[3]}')

        self.db_handler.insert_raw_responses(
            '/leagues',
            {'id':39},
            league
        ) 
       
        return league
    
# -- ============================================================================
# -- FETCH AND STORE TEAMS STARTS
# -- ============================================================================
    def fetch_and_store_teams(self):
        logger.info('Fetching EPL teams data...')
        teams = self.api_client.get_teams(league_id=39, season=2023)

        if not teams:
            logger.warning('No teams data fetched')
            return False
        
        logger.info(f"Fetched {teams.get('results', 0)} teams")

        # Store raw response
        self.db_handler.insert_raw_responses(
            '/teams',
            {'league': 39, 'season': 2023},
            teams
        )
        return True
        
    def fetch_and_store_teams_multi_season(self, seasons=None):

        if seasons is None:
            # Default: last 5 seasons
            current_year = 2024
            seasons = list(range(current_year - 4, current_year + 1))
        logger.info(f'Fetching EPL teams for multiple seasons: {seasons}')

        results = {
            'seasons_fetched': 0,
            'total_teams': 0,
            'failed_seasons': []
        }

        for season in seasons:
            logger.info(f'Fetching teams for season {season}...')
            try:
                teams = self.api_client.get_teams(league_id=39, season=season)
                
                if not teams or teams.get('results', 0) == 0:
                    logger.warning(f'No teams data for season {season}')
                    results['failed_seasons'].append(season)
                    continue
                
                teams_count = teams.get('results', 0)
                logger.info(f"Season {season}: Fetched {teams_count} teams")
                
                # Store raw response
                self.db_handler.insert_raw_responses(
                    '/teams',
                    {'league': 39, 'season': season},
                    teams
                )
                results['seasons_fetched'] += 1
                results['total_teams'] += teams_count
            
            except Exception as e:
                logger.error(f"Error fetching teams for season {season}: {e}")
                results['failed_seasons'].append(season)
                continue

            logger.info(f"Multi-season fetch complete: {results}")
        return results
        
    def fetch_and_store_all_epl_teams_historical(self):
        logger.info('Fetching ALL historical EPL teams (2010-2024)...')
        # EPL seasons from 2010 to 2024
        seasons = list(range(2021, 2025))  # 2010, 2011, ..., 2024
        return self.fetch_and_store_teams_multi_season(seasons=seasons)
# -- ============================================================================
# -- FETCH AND STORE TEAMS END
# -- ============================================================================
    
    
# -- ============================================================================
# -- FETCH AND STORE FIXTURES
# -- ============================================================================
    def fetch_and_store_fixtures(self, seasons, status=None):

        if seasons is None:
            # Default: last 5 seasons
            current_year = 2024
            seasons = list(range(current_year - 4, current_year + 1))
        logger.info(f'Fetching EPL fixtures for multiple seasons: {seasons}')

        results = {
            'seasons_fetched': 0,
            'total_teams': 0,
            'failed_seasons': []
        }

        if status:
            logger.info(f'Filtering by status: {status}')

        for season in seasons:
            try:
                logger.info(f'Fetching teams for season {season}...')   
                fixtures = self.api_client.get_fixtures(39,season,status)
            
                if not fixtures:
                    logger.warning('No fixtures data fetched')
                    results['failed_seasons'].append(season)
                    continue
            
                results_count = fixtures.get('results', 0)
                logger.info(f"Fetched {results_count} fixtures")

                # Store raw response
                self.db_handler.insert_raw_responses(
                    '/fixtures',
                    {'league': 39, 'season': season, 'status': status},
                    fixtures
                )

            except Exception as e:
                logger.error(f"Error fetching fixtures for season {season}: {e}")
                results['failed_seasons'].append(season)

            time.sleep(7.0) # Rate limiting for 10req/min tier

            logger.info(f"Multi-season fixtures fetch complete: {results}")
        return results
    
    def fetch_and_store_all_epl_fixtures_historical(self):
        logger.info('Fetching ALL historical EPL fixtures (2010-2024)...')
        # EPL seasons from 2010 to 2024
        seasons = list(range(2021, 2025))  # 2010, 2011, ..., 2024
        return self.fetch_and_store_fixtures(seasons=seasons)

# -- ============================================================================
# -- FETCH AND STORE FIXTURES END
# -- ============================================================================


# -- ============================================================================
# -- FETCH AND STORE PLAYERS STARTS
# -- ============================================================================    
    def fetch_and_store_player_stats(self, limit=30):
        """
        Fetch player stats for completed matches that haven't been fetched yet.
        Checks raw_api_responses to see if we already have data for a fixture.
        """
        logger.info(f"Fetching player stats (limit={limit})...")
        
        # 1. Find completed matches (FT, AET, PEN)
        # 2. Exclude matches where we already have a raw response in raw_api_responses
        query = """
            SELECT m.fixture_id, m.match_date
            FROM matches m
            LEFT JOIN raw_api_responses r 
                ON r.endpoint = '/fixtures/players' 
                AND (r.request_params->>'fixture')::int = m.fixture_id
            WHERE m.status IN ('FT', 'AET', 'PEN')
            AND r.response_id IS NULL
            ORDER BY m.match_date DESC
            LIMIT %s
        """
        
        results = self.db_handler.execute_query(query, (limit,))
        
        if not results:
            logger.info("No new matches found needing player stats.")
            return 0
            
        fixture_ids = [row[0] for row in results]
        logger.info(f"Found {len(fixture_ids)} matches needing stats. Fetching...")
        
        count = 0
        import time
        
        for fixture_id in fixture_ids:
            try:
                stats = self.api_client.get_fixture_player_statistics(fixture_id)
                
                if stats:
                    self.db_handler.insert_raw_responses(
                        '/fixtures/players',
                        {'fixture': fixture_id},
                        stats
                    )
                    count += 1
                
                time.sleep(7.0) # Rate limiting for 10req/min tier
                
            except Exception as e:
                logger.error(f"Error fetching stats for fixture {fixture_id}: {e}")
                continue
                
        logger.info(f"Successfully fetched stats for {count} matches.")
        return count
    
    def fetch_and_store_player_profiles(self, season=2024, max_pages=50):
        """
        Fetch player data from /players endpoint filtered by EPL league for a specific season.
        """
        logger.info(f"Fetching EPL player profiles for season {season}...")
        
        total_fetched = 0
        import time
        
        for page in range(1, max_pages + 1):
            try:
                data = self.api_client.get_player_season_stats(
                    league_id=39, 
                    season=season,
                    page=page
                )
                
                if not data or not data.get('response'):
                    logger.info(f"No more player data at page {page} for season {season}")
                    break
                    
                self.db_handler.insert_raw_responses(
                    '/players',
                    {'league': 39, 'season': season, 'page': page, 'type': 'profile_sync'},
                    data
                )
                
                results_count = len(data.get('response', []))
                total_fetched += results_count
                
                paging = data.get('paging', {})
                if paging.get('current', 1) >= paging.get('total', 1):
                    logger.info("Reached last page of players")
                    break
                    
                time.sleep(7.0) # Rate limiting for 10req/min tier
                
            except Exception as e:
                logger.error(f"Error fetching player profiles page {page} for season {season}: {e}")
                continue
                
        return total_fetched

    def fetch_and_store_player_profiles_multi_season(self, seasons=None):
        """
        Fetch player profiles for multiple EPL seasons.
        """
        if seasons is None:
            # Default to last 3 seasons
            current_year = 2024
            seasons = [current_year - 2, current_year - 1, current_year]
            
        logger.info(f"Starting multi-season player fetching for: {seasons}")
        results = {'total_fetched': 0, 'seasons_completed': []}
        
        for season in seasons:
            count = self.fetch_and_store_player_profiles(season=season)
            results['total_fetched'] += count
            results['seasons_completed'].append(season)
            
        logger.info(f"Multi-season player fetch complete: {results}")
        return results

    def fetch_and_store_missing_player_profiles(self, limit=80):
        """
        Identify players in dim_players that only have a name/skeleton data 
        (e.g., from JIT creation in stats) and fetch their full details.
        """
        logger.info(f"Looking for missing player profiles (limit={limit})...")
        
        # Query for players with missing firstname (skeleton records)
        query = """
            SELECT player_id, player_name 
            FROM dim_players 
            WHERE firstname IS NULL
            LIMIT %s
        """
        results = self.db_handler.execute_query(query, (limit,))
        
        if not results:
            logger.info("No skeleton player profiles found.")
            return 0
            
        count = 0
        for row in results:
            player_id, player_name = row
            try:
                # Use current year for a generic profile check
                data = self.api_client.get_player_season_stats(player_id=player_id, season=2024)
                
                if data and data.get('response'):
                    self.db_handler.insert_raw_responses(
                        '/players',
                        {'player': player_id, 'season': 2024, 'type': 'profile_repair'},
                        data
                    )
                    count += 1
                
                # Stay within 10 req/min limit
                time.sleep(7.0)
                
            except Exception as e:
                logger.error(f"Error repairing profile for player {player_id} ({player_name}): {e}")
                continue
                
        logger.info(f"Successfully fetched {count} skeleton profiles for repair.")
        return count

    def fetch_and_store_standings(self, season=2024):
        """Fetch and store league standings."""
        logger.info(f"Fetching standings for season {season}...")
        data = self.api_client.get_standings(season=season)
        
        if data and data.get('response'):
            # Store raw
            self.db_handler.insert_raw_responses(
                '/standings',
                {'league': self.api_client.league_id, 'season': season},
                data
            )
            return True
        else:
            logger.warning("No standings data found.")
            return False

    def fetch_and_store_standings_multi_season(self, seasons=None):
        if seasons is None:
            seasons = [2024]
        
        logger.info(f"Fetching standings for multiple seasons: {seasons}")
        results = {"seasons_fetched": 0, "failed_seasons": []}
        
        for season in seasons:
            if self.fetch_and_store_standings(season):
                results["seasons_fetched"] += 1
            else:
                results["failed_seasons"].append(season)
            time.sleep(7.0)
            
        return results

    def fetch_and_store_all_epl_standings_historical(self):
        logger.info("Fetching historical EPL standings (2021-2024)...")
        seasons = list(range(2021, 2025))
        return self.fetch_and_store_standings_multi_season(seasons=seasons)

# -- ============================================================================
# -- FETCH AND STORE PLAYERS ENDS
# -- ============================================================================  
    

    