import requests
from datetime import datetime, timedelta
import json
import time
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.configs import config
from src.utils.logger import setup_logger

logger = setup_logger(__name__, 'ingestion.log')
class FootballAPIClient:
    def __init__(self):
        self.base_url = config.FOOTBALL_API_BASE_URL
        self.headers = {
            "x-apisports-key": config.FOOTBALL_API_KEY
        }
        self.league_id = config.EPL_LEAGUE_ID
    
    def make_request(self, endpoint, params, max_retries: int = 3):
        url = f'{self.base_url}/{endpoint}'

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=30
                )
                
                # Check for 429 status code first
                if response.status_code == 429:
                    logger.warning(f"Rate limit hit (429). Waiting 60s... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(60)
                    continue

                data = response.json()
                
                # API-Football often returns 200 OK but with an errors object for rate limits
                if 'errors' in data and data['errors']:
                    errors = data['errors']
                    # Handle specific rate limit error in body
                    if isinstance(errors, dict) and 'rateLimit' in errors:
                        logger.warning(f"API Rate Limit error in body: {errors['rateLimit']}. Waiting 60s...")
                        time.sleep(60)
                        continue
                    
                    logger.error(f"API returned errors: {errors}")
                    return None

                # Proactive rate limiting: check headers if provided by API-Sports
                # Standard practice for API-Sports is x-ratelimit-requests-remaining
                remaining = response.headers.get('x-ratelimit-requests-remaining')
                if remaining is not None and int(remaining) <= 1:
                    logger.warning("Only 1 request remaining in current minute. Slowing down...")
                    time.sleep(10)

                logger.info(f"Successfully fetched data from {endpoint}")
                return data
            
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                        
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    return None
                        
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)

        logger.error(f"Failed to fetch data from {endpoint} after {max_retries} attempts")
        return None
    
    def get_league(self):
         data = self.make_request('leagues', {'id':39})
         return data
    
    def get_teams(self, league_id=None, season=None):
        params = {
            'league': league_id or self.league_id,
            'season': season or self.current_season
        } 
        data = self.make_request("teams", params)
        return data
    
    def get_fixtures(self, league_id=None, season=None, status=None):
        params = {
            'league': league_id or self.league_id,
            'season': season or 2024
        }

        if status:
             params['status'] = status
        logger.info(f"Fetching fixtures for league {params['league']}, season {params['season']}")

        data = self.make_request('fixtures', params)
        return data

    def get_fixture_player_statistics(self, fixture_id):
        """Fetch detailed player statistics for a specific fixture."""
        params = {'fixture': fixture_id}
        logger.info(f"Fetching player stats for fixture {fixture_id}")
        return self.make_request('fixtures/players', params)

    def get_player_profiles(self, player_id=None, search=None, page=1):
        """
        Fetch player profiles from /players/profiles endpoint.
        Returns: id, name, firstname, lastname, age, birth, nationality, height, weight, position, photo
        Pagination: 250 results per page.
        """
        params = {'page': page}
        if player_id:
            params['player'] = player_id
        if search:
            params['search'] = search
        logger.info(f"Fetching player profiles (page {page})")
        return self.make_request('players/profiles', params)

    def get_player_season_stats(self, league_id=None, season=None, team_id=None, player_id=None, page=1):
        """
        Fetch player season statistics from /players endpoint.
        Returns aggregated stats: games, goals, assists, cards, etc. per season.
        Pagination: 20 results per page.
        """
        params = {'page': page}
        if league_id:
            params['league'] = league_id
        if season:
            params['season'] = season
        if team_id:
            params['team'] = team_id
        if player_id:
            params['player'] = player_id
            
        logger.info(f"Fetching player season stats (id={player_id}, league={league_id}, season={season}, page={page})")
        return self.make_request('players', params)

    def get_standings(self, league_id=None, season=None):
        """Fetch standings for a league and season."""
        params = {
            'league': league_id or self.league_id,
            'season': season or 2024
        }
        logger.info(f"Fetching standings for league {params['league']}, season {params['season']}")
        return self.make_request('standings', params)

    # def get_fixtures(self, league_id, season):
    #      params = {
    #           'league':league_id,
    #           'season': season,   
    #      }
    #      data = self.make_request("fixtures", params)
    #      return data
    

# if __name__ == "__main__":
#     # Example usage
#     get_league_data = FootballAPIClient()
#     league = json.dumps(get_league_data.get_league(), indent=4)
#     print(league)
