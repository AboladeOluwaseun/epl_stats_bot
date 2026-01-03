import sys
from pathlib import Path
from fastapi import FastAPI, Form, Request, Response
from typing import Optional

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.bot.query_engine import QueryEngine
from src.bot.whatsapp_service import WhatsAppService
from src.utils.logger import setup_logger

logger = setup_logger(__name__, 'bot.log')

app = FastAPI(title="EPL Stats WhatsApp Bot")

# Initialize services
query_engine = QueryEngine()
whatsapp = WhatsAppService()

@app.post("/webhook")
async def handle_whatsapp_webhook(
    From: str = Form(...),
    Body: str = Form(...)
):
    """Webhook to handle incoming messages from Twilio."""
    user_message = Body.strip().lower()
    sender = From
    
    logger.info(f"Received message from {sender}: {user_message}")
    
    response_msg = ""
    media_url = None

    try:
        if user_message in ['hi', 'hello', 'start', 'help']:
            response_msg = (
                "👋 *Welcome to the EPL Stats Bot!*\n\n"
                "I can help you with player and team statistics.\n\n"
                "Try sending:\n"
                "• A player name (e.g., *Haaland*)\n"
                "• A team name (e.g., *Arsenal*)\n"
                "• *'Matches'* to see recent results"
            )
        
        elif user_message.startswith('match') or ' vs ' in user_message:
            # Handle "match Arsenal vs Chelsea" or just "Arsenal vs Chelsea"
            query_str = user_message
            if user_message.startswith('match '):
                query_str = user_message.replace('match ', '', 1)
            
            if ' vs ' in query_str:
                parts = query_str.split(' vs ')
                if len(parts) >= 2:
                    t1, t2 = parts[0].strip(), parts[1].strip()
                    matches = query_engine.search_fixture(t1, t2)
                    from src.bot.formatter import format_head_to_head
                    response_msg = format_head_to_head(t1, t2, matches)
                else:
                    response_msg = "⚠️ Please specify two teams separated by 'vs'. Example: *Arsenal vs Chelsea*"
            else:
                response_msg = "⚠️ To compare teams, use 'vs'. Example: *Arsenal vs Chelsea*"

        elif 'table' in user_message or 'standings' in user_message:
            # Extract potential year
            season = None
            import re
            match = re.search(r'\b(20\d{2})\b', user_message)
            if match:
                season = int(match.group(1))
                
            standings = query_engine.get_latest_standings(season)
            from src.bot.formatter import format_standings
            
            if not standings:
                 response_msg = f"📉 No standings data available for season {season or 'latest'}."
            else:
                response_msg = format_standings(standings[:15]) # Top 15

        else:
            # 1. Try searching for a player
            players = query_engine.search_player(user_message)
            
            if players:
                if len(players) == 1:
                    # Exactly one player found
                    player = players[0]
                    # Fetch latest stats as a separate add-on
                    stats = query_engine.get_player_latest_stats(player['id'])
                    
                    response_msg = whatsapp.format_player_stats(player, stats)
                    media_url = player['photo']
                else:
                    # Multiple players found
                    response_msg = "I found multiple players. Did you mean one of these?\n\n"
                    for p in players:
                        response_msg += f"• {p['name']} ({p['position']})\n"
                    response_msg += "\nPlease search with their full name for better results."
            
            else:
                # 2. Try searching for a team
                results = query_engine.get_team_latest_results(user_message)
                if results:
                    response_msg = whatsapp.format_team_results(user_message.capitalize(), results)
                else:
                    response_msg = (
                        "Sorry, I couldn't find any players or teams matching your search. 😕\n\n"
                        "Try a different name or type *'help'* for instructions."
                    )

        # Send response via WhatsApp
        whatsapp.send_message(sender, response_msg, media_url)
        
        # Twilio expects an empty TwiML response if we send the message via the API
        return Response(content='<?xml version="1.0" encoding="UTF-8"?><Response></Response>', media_type="application/xml")

    except Exception as e:
        logger.error(f"Error in webhook handler: {e}")
        return Response(content='<?xml version="1.0" encoding="UTF-8"?><Response></Response>', media_type="application/xml")

if __name__ == "__main__":
    import uvicorn
    # reload=True ensures the server restarts automatically when code changes
    uvicorn.run("src.bot.app:app", host="0.0.0.0", port=5000, reload=True)
