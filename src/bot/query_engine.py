import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.storage.postgres_handler import PostgresHandler
from src.utils.logger import setup_logger

logger = setup_logger(__name__, 'bot.log')

class QueryEngine:
    """Engine to query the database for bot responses."""
    
    def __init__(self):
        self.db = PostgresHandler()

    def search_player(self, name_query: str) -> List[Dict[str, Any]]:
        """Search for a player by name, returning detailed profile info."""
        query = """
            SELECT 
                player_id, player_name, nationality, position, photo_url,
                age, height, weight, number, firstname, lastname
            FROM dim_players
            WHERE player_name ILIKE %s
            LIMIT 5
        """
        search_term = f"%{name_query}%"
        results = self.db.execute_query(query, (search_term,))
        
        players = []
        if results:
            for row in results:
                players.append({
                    'id': row[0],
                    'name': row[1],
                    'nationality': row[2],
                    'position': row[3],
                    'photo': row[4],
                    'age': row[5],
                    'height': row[6],
                    'weight': row[7],
                    'number': row[8],
                    'firstname': row[9],
                    'lastname': row[10]
                })
        return players

    def get_player_latest_stats(self, player_id: int) -> Optional[Dict[str, Any]]:
        """Get the most recent match stats for a player."""
        query = """
            SELECT 
                m.match_date,
                t.team_name as team,
                match_teams.home_team_name,
                match_teams.away_team_name,
                s.minutes_played,
                s.rating,
                s.goals_total,
                s.assists,
                s.passes_accuracy,
                s.shots_on_target
            FROM fact_player_stats s
            JOIN matches m ON s.fixture_id = m.fixture_id
            JOIN dim_teams t ON s.team_id = t.team_id
            JOIN (
                SELECT m2.fixture_id, ht.team_name as home_team_name, at.team_name as away_team_name
                FROM matches m2
                JOIN dim_teams ht ON m2.home_team_id = ht.team_id
                JOIN dim_teams at ON m2.away_team_id = at.team_id
            ) match_teams ON s.fixture_id = match_teams.fixture_id
            WHERE s.player_id = %s
            ORDER BY m.match_date DESC
            LIMIT 1
        """
        results = self.db.execute_query(query, (player_id,))
        
        if results:
            row = results[0]
            return {
                'date': row[0].strftime('%Y-%m-%d'),
                'team': row[1],
                'matchup': f"{row[2]} vs {row[3]}",
                'minutes': row[4],
                'rating': float(row[5]) if row[5] else 0.0,
                'goals': row[6],
                'assists': row[7],
                'passes_acc': row[8],
                'shots_on_target': row[9]
            }
        return None

    def get_team_latest_results(self, team_name: str) -> List[Dict[str, Any]]:
        """Get the latest results for a specific team."""
        query = """
            SELECT 
                m.match_date,
                ht.team_name as home_team,
                at.team_name as away_team,
                m.home_goals,
                m.away_goals,
                m.status
            FROM matches m
            JOIN dim_teams ht ON m.home_team_id = ht.team_id
            JOIN dim_teams at ON m.away_team_id = at.team_id
            WHERE ht.team_name ILIKE %s OR at.team_name ILIKE %s
            AND m.status = 'FT'
            ORDER BY m.match_date DESC
            LIMIT 3
        """
        search_term = f"%{team_name}%"
        results = self.db.execute_query(query, (search_term, search_term))
        
        matches = []
        if results:
            for row in results:
                matches.append({
                    'date': row[0].strftime('%Y-%m-%d'),
                    'home_team': row[1],
                    'away_team': row[2],
                    'home_goals': row[3],
                    'away_goals': row[4],
                    'status': row[5]
                })
        return matches

    def search_fixture(self, team1_name: str, team2_name: str) -> List[Dict[str, Any]]:
        """Search for head-to-head matches between two teams."""
        query = """
            SELECT 
                m.match_date,
                m.season,
                ht.team_name as home_team,
                at.team_name as away_team,
                m.home_goals,
                m.away_goals,
                m.status,
                v.venue_name
            FROM matches m
            JOIN dim_teams ht ON m.home_team_id = ht.team_id
            JOIN dim_teams at ON m.away_team_id = at.team_id
            LEFT JOIN dim_venues v ON m.venue_id = v.venue_id
            WHERE (ht.team_name ILIKE %s AND at.team_name ILIKE %s)
               OR (ht.team_name ILIKE %s AND at.team_name ILIKE %s)
            ORDER BY m.match_date DESC
            LIMIT 5
        """
        t1 = f"%{team1_name}%"
        t2 = f"%{team2_name}%"
        # We need to check both permutations: T1 vs T2 AND T2 vs T1
        results = self.db.execute_query(query, (t1, t2, t2, t1))
        
        matches = []
        if results:
            for row in results:
                matches.append({
                    'date': row[0].strftime('%Y-%m-%d'),
                    'season': row[1],
                    'home_team': row[2],
                    'away_team': row[3],
                    'home_goals': row[4],
                    'away_goals': row[5],
                    'status': row[6],
                    'venue': row[7] or 'Unknown Venue'
                })
        return matches

    def get_latest_standings(self, season: int = None) -> List[Dict[str, Any]]:
        """Get the latest league standings."""
        # If season not provided, find the max season in fact_standings
        if not season:
            season_query = "SELECT MAX(season) FROM fact_standings"
            res = self.db.execute_query(season_query)
            if res and res[0][0]:
                season = res[0][0]
            else:
                return []
        
        query = """
            SELECT 
                fs.rank,
                t.team_name,
                fs.played,
                fs.win,
                fs.draw,
                fs.lose,
                fs.goals_diff,
                fs.points,
                fs.form
            FROM fact_standings fs
            JOIN dim_teams t ON fs.team_id = t.team_id
            WHERE fs.league_id = 39 AND fs.season = %s
            ORDER BY fs.rank ASC
        """
        results = self.db.execute_query(query, (season,))
        
        table = []
        if results:
            for row in results:
                table.append({
                    'rank': row[0],
                    'team': row[1],
                    'played': row[2],
                    'win': row[3],
                    'draw': row[4],
                    'lose': row[5],
                    'gd': row[6],
                    'points': row[7],
                    'form': row[8]
                })
        return table
