import sys
from pathlib import Path
from whatsappy import Whatsapp
from whatsappy.events import MessageEvent
import time

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.bot.query_engine import QueryEngine
from src.bot.formatter import format_player_stats, format_team_results
from src.utils.logger import setup_logger

logger = setup_logger(__name__, 'group_bot.log')

# Initialize the Bot with a session name to keep us logged in
# This will open a Chrome window for the QR scan
whatsapp = Whatsapp()

query_engine = QueryEngine()

@whatsapp.event
def on_ready():
    logger.info("WhatsApp Group Bot is ready and logged in!")
    print("\n✅ Bot is ready! You can now use it in your groups.")
    print("Commands: !stats [player name], !results [team name], !help")

@whatsapp.event
def on_message(chat, message):
    text = message.content.strip()
    
    # We only care about messages starting with '!'
    if not text.startswith('!'):
        return

    logger.info(f"Command received in '{chat.name}': {text}")
    command_parts = text.split(' ', 1)
    cmd = command_parts[0].lower()
    query = command_parts[1].strip() if len(command_parts) > 1 else ""

    response_msg = ""

    try:
        if cmd == '!help':
            response_msg = (
                "🤖 *EPL Stats Group Bot*\n\n"
                "• `!stats [player]` - Get player profile and latest match stats\n"
                "• `!results [team]` - Get latest team results\n"
                "• `!hi` - Say hello"
            )

        elif cmd == '!hi':
            response_msg = f"👋 Hello {chat.name}! I'm ready to provide EPL data. Try `!stats Haaland`"

        elif cmd == '!stats':
            if not query:
                response_msg = "⚠️ Please provide a player name. Example: `!stats Salah`"
            else:
                players = query_engine.search_player(query)
                if not players:
                    response_msg = f"🔍 Sorry, I couldn't find any player matching *'{query}'*."
                elif len(players) > 1:
                    response_msg = "🤔 I found multiple players. Could you be more specific?\n\n"
                    for p in players[:3]:
                        response_msg += f"• {p['name']} ({p['position']})\n"
                else:
                    player = players[0]
                    stats = query_engine.get_player_latest_stats(player['id'])
                    response_msg = format_player_stats(player, stats)

        elif cmd == '!results':
            if not query:
                response_msg = "⚠️ Please provide a team name. Example: `!results Chelsea`"
            else:
                results = query_engine.get_team_latest_results(query)
                if not results:
                    response_msg = f"🔍 No results found for *'{query}'*."
                else:
                    response_msg = format_team_results(query.capitalize(), results)

        elif cmd == '!match':
            # Expected format: !match TeamA TeamB or !match TeamA vs TeamB
            if not query:
                response_msg = "⚠️ Please provide two teams. Example: `!match Arsenal Chelsea`"
            else:
                # Basic splitting logic
                if ' vs ' in query:
                    teams = query.split(' vs ')
                else:
                    teams = query.split()
                
                if len(teams) < 2:
                     response_msg = "⚠️ I couldn't identify two teams. Try: `!match Arsenal vs Chelsea`"
                else:
                    # Take the first two parts if simple split, or use split logic
                    # A better approach for "Man City vs Home" is split by ' vs ' or handle multi-word names responsibly
                    # For now, let's assume ' vs ' is best, or user inputs simpler names.
                    # Creating a slightly robust parser for the space-separated one is tricky without NLP.
                    # We'll stick to ' vs ' as preferred, or first and last words if no vs.
                    
                    if ' vs ' in query:
                        t1, t2 = query.split(' vs ', 1)
                    else:
                        # Fallback for simple names "Arsenal Chelsea"
                        parts = query.split()
                        if len(parts) == 2:
                            t1, t2 = parts[0], parts[1]
                        else:
                             # Hard to guess split for "Man City Chelsea" -> Man, City Chelsea?
                             # Let's ask user to use 'vs' for multiword
                             response_msg = "⚠️ For teams with spaces, please use 'vs'. Example: `!match Man City vs Chelsea`"
                             t1, t2 = None, None
                    
                    if t1 and t2:
                        from src.bot.formatter import format_head_to_head # Import here to avoid circular if any
                        response_msg = format_head_to_head(t1.strip(), t2.strip(), matches)

        elif cmd == '!table':
            season = None
            if query and query.isdigit() and len(query) == 4:
                season = int(query)
            
            standings = query_engine.get_latest_standings(season)
            from src.bot.formatter import format_standings
            
            if not standings:
                response_msg = f"📉 No standings data available for season {season or 'latest'}."
            else:
                response_msg = format_standings(standings[:15])
                if len(standings) > 15:
                    response_msg += f"\n_...and {len(standings)-15} more teams._"

        if response_msg:
            # Send the message back to the same chat (group or individual)
            chat.send(response_msg)
            logger.info(f"Response sent to {chat.name}")

    except Exception as e:
        logger.error(f"Error processing command {text}: {e}")
        chat.send("❌ Sorry, I encountered an error while fetching that data.")

if __name__ == "__main__":
    print("\n🚀 Starting WhatsApp Group Bot...")
    print("👉 A Chrome window will open. Please scan the QR code to log in.")
    
    # Run the bot
    # This will block and keep the script running
    whatsapp.run()
