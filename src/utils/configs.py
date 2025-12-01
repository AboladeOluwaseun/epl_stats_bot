"""Configuration management using environment variables."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

class Config:
    """Application configuration."""
    
    # API Keys
    FOOTBALL_API_KEY = os.getenv('FOOTBALL_API_KEY')
    FOOTBALL_API_BASE_URL = os.getenv('FOOTBALL_API_BASE_URL')
    EPL_LEAGUE_ID = os.getenv('EPL_LEAGUE_ID', 39)
    
    # # Database
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'epl_stats')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    
    # @property
    # def database_url(self):
    #     return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    # # AWS
    # AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    # AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    # S3_BUCKET = os.getenv('S3_BUCKET', 'epl-stats-data')
    
    # # Bot
    # TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    
    # # Spark
    # SPARK_APP_NAME = 'EPL-Stats-Pipeline'
    
config = Config()

