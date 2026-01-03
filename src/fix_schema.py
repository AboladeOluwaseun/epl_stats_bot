import os
import psycopg2

def migrate():
    try:
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'postgres'),
            database=os.environ.get('POSTGRES_DB', 'epl_stats'),
            user=os.environ.get('POSTGRES_USER', 'postgres'),
            password=os.environ.get('POSTGRES_PASSWORD', 'postgres')
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        print("Dropping existing tables...")
        cur.execute("DROP TABLE IF EXISTS fact_player_stats CASCADE")
        cur.execute("DROP TABLE IF EXISTS dim_players CASCADE")
        
        print("Creating dim_players...")
        cur.execute("""
            CREATE TABLE dim_players (
                player_id BIGINT PRIMARY KEY,
                player_name VARCHAR(100) NOT NULL,
                firstname VARCHAR(100),
                lastname VARCHAR(100),
                age INTEGER,
                birth_date DATE,
                birth_place VARCHAR(100),
                birth_country VARCHAR(100),
                nationality VARCHAR(100),
                height VARCHAR(20),
                weight VARCHAR(20),
                number INTEGER,
                position VARCHAR(50),
                photo_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print("Creating fact_player_stats...")
        cur.execute("""
            CREATE TABLE fact_player_stats (
                stat_id SERIAL PRIMARY KEY,
                fixture_id INTEGER NOT NULL REFERENCES matches(fixture_id),
                player_id BIGINT NOT NULL REFERENCES dim_players(player_id),
                team_id INTEGER NOT NULL REFERENCES dim_teams(team_id),
                minutes_played INTEGER,
                rating NUMERIC(4, 2),
                captain BOOLEAN DEFAULT FALSE,
                substitute BOOLEAN DEFAULT FALSE,
                offside INTEGER,
                shots_total INTEGER,
                shots_on_target INTEGER,
                goals_total INTEGER,
                goals_conceded INTEGER,
                assists INTEGER,
                saves INTEGER,
                passes_total INTEGER,
                passes_key INTEGER,
                passes_accuracy VARCHAR(10),
                tackles_total INTEGER,
                blocks INTEGER,
                interceptions INTEGER,
                duels_total INTEGER,
                duels_won INTEGER,
                dribbles_attempts INTEGER,
                dribbles_success INTEGER,
                dribbles_past INTEGER,
                fouls_drawn INTEGER,
                fouls_committed INTEGER,
                yellow_cards INTEGER,
                red_cards INTEGER,
                penalty_won INTEGER,
                penalty_commited INTEGER,
                penalty_scored INTEGER,
                penalty_missed INTEGER,
                penalty_saved INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(fixture_id, player_id)
            )
        """)
        
        print("Schema migration successful!")
        
        # Verify columns
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'dim_players'")
        columns = [r[0] for r in cur.fetchall()]
        print(f"dim_players columns: {columns}")
        
        conn.close()
    except Exception as e:
        print(f"Migration failed: {e}")

if __name__ == '__main__':
    migrate()
