from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.storage.postgres_handler import PostgresHandler
from src.utils.logger import setup_logger

logger = setup_logger(__name__, 'schema_update.log')

def update_schema():
    logger.info("Starting schema update...")
    handler = PostgresHandler()
    
    # DDL for new tables
    ddl = """
    -- ============================================================================
    -- DIMENSION TABLE: Players
    -- ============================================================================
    CREATE TABLE IF NOT EXISTS dim_players (
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
    );
    
    -- Add/alter columns for existing tables
    DO $$ 
    BEGIN 
        -- Add position column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dim_players' AND column_name='position') THEN
            ALTER TABLE dim_players ADD COLUMN position VARCHAR(50);
        END IF;
        -- Add number column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dim_players' AND column_name='number') THEN
            ALTER TABLE dim_players ADD COLUMN number INTEGER;
        END IF;
        -- Change player_id to BIGINT if it's INTEGER
        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dim_players' AND column_name='player_id' AND data_type='integer') THEN
            ALTER TABLE dim_players ALTER COLUMN player_id TYPE BIGINT;
        END IF;
    END $$;

    -- ============================================================================
    -- FACT TABLE: Player Match Stats
    -- ============================================================================
    CREATE TABLE IF NOT EXISTS fact_player_stats (
        stat_id SERIAL PRIMARY KEY,
        fixture_id INTEGER NOT NULL REFERENCES matches(fixture_id),
        player_id BIGINT NOT NULL REFERENCES dim_players(player_id),
        team_id INTEGER NOT NULL REFERENCES dim_teams(team_id),
        
        -- General
        minutes_played INTEGER,
        rating NUMERIC(4, 2),
        captain BOOLEAN DEFAULT FALSE,
        substitute BOOLEAN DEFAULT FALSE,
        
        -- Offense
        offside INTEGER,
        shots_total INTEGER,
        shots_on_target INTEGER,
        goals_total INTEGER,
        goals_conceded INTEGER,
        assists INTEGER,
        saves INTEGER,
        
        -- Passing
        passes_total INTEGER,
        passes_key INTEGER,
        passes_accuracy INTEGER,
        
        -- Defense
        tackles_total INTEGER,
        blocks INTEGER,
        interceptions INTEGER,
        
        -- Duels
        duels_total INTEGER,
        duels_won INTEGER,
        
        -- Dribbles
        dribbles_attempts INTEGER,
        dribbles_success INTEGER,
        dribbles_past INTEGER,
        
        -- Fouls
        fouls_drawn INTEGER,
        fouls_committed INTEGER,
        
        -- Cards
        yellow_cards INTEGER,
        red_cards INTEGER,
        
        -- Penalty
        penalty_won INTEGER,
        penalty_commited INTEGER,
        penalty_scored INTEGER,
        penalty_missed INTEGER,
        penalty_saved INTEGER,
        
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        UNIQUE(fixture_id, player_id) 
    );

    CREATE INDEX IF NOT EXISTS idx_player_stats_fixture ON fact_player_stats(fixture_id);
    CREATE INDEX IF NOT EXISTS idx_player_stats_player ON fact_player_stats(player_id);
    CREATE INDEX IF NOT EXISTS idx_player_stats_team ON fact_player_stats(team_id);
    """
    
    try:
        # We need to execute inside a connection block
        with handler.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
        logger.info("Schema update completed successfully!")
        
    except Exception as e:
        logger.error(f"Failed to update schema: {e}")
        raise

if __name__ == '__main__':
    update_schema()
