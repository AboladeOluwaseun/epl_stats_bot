import sys
from pathlib import Path
from twilio.rest import Client
import os

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.configs import config
from src.utils.logger import setup_logger
from src.bot import formatter

logger = setup_logger(__name__, 'bot.log')

class WhatsAppService:
    """Service to send WhatsApp messages via Twilio."""
    
    def __init__(self):
        self.client = Client(config.TWILIO_ACCOUNT_SID, config.TWILIO_AUTH_TOKEN)
        self.from_number = config.TWILIO_WHATSAPP_NUMBER

    def send_message(self, to_number: str, message: str, media_url: str = None):
        """Send a WhatsApp message."""
        try:
            params = {
                "body": message,
                "from_": self.from_number,
                "to": to_number
            }
            if media_url:
                params["media_url"] = [media_url]

            msg = self.client.messages.create(**params)
            logger.info(f"Message sent to {to_number}: {msg.sid}")
            return msg.sid
        except Exception as e:
            logger.error(f"Error sending WhatsApp message: {e}")
            return None

    def format_player_stats(self, player: dict, stats: dict = None) -> str:
        return formatter.format_player_stats(player, stats)

    def format_team_results(self, team_name: str, results: list) -> str:
        return formatter.format_team_results(team_name, results)
