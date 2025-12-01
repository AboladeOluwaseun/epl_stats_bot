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
    
    def make_request (self, endpoint, params, max_retries: int =3):
        url =  f'{self.base_url}/{endpoint}'

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url,
                    headers = self.headers,
                    params = params
                )
                data = response.json()
                # Check API-specific error handling
                if 'errors' in data and data['errors']:
                    logger.error(f"API returned errors: {data['errors']}")
                    return None
                logger.info(f"Successfully fetched data from {endpoint}")
                logger.debug(f"Results count: {data.get('results', 0)}")
                return data
            
            except requests.exceptions.Timeout:
                    logger.warning(f"Timeout on attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        
            except requests.exceptions.HTTPError as e:
                    logger.error(f"HTTP error: {e}")
                    if e.response.status_code == 429:  # Rate limit
                        logger.warning("Rate limit hit, waiting 60 seconds")
                        time.sleep(60)
                    elif e.response.status_code >= 500:  # Server error
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
    def get_fixtures(self, league_id, season):
         params = {
              'league':league_id,
              'season': season,   
         }
         data = self.make_request("fixtures", params)
         return data
    

if __name__ == "__main__":
    # Example usage
    get_league_data = FootballAPIClient()
    league = json.dumps(get_league_data.get_league(), indent=4)
    print(league)
