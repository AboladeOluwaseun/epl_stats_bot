-- =============================================================================
-- DROP EXISTING TABLES (for clean setup)
-- =============================================================================
DROP TABLE IF EXISTS fact_player_stats CASCADE;
DROP TABLE IF EXISTS fact_goals CASCADE;
DROP TABLE IF EXISTS fact_matches CASCADE;
DROP TABLE IF EXISTS dim_players CASCADE;
DROP TABLE IF EXISTS dim_teams CASCADE;
DROP TABLE IF EXISTS dim_seasons CASCADE;
DROP TABLE IF EXISTS dim_venues CASCADE;
DROP TABLE IF EXISTS raw_api_responses CASCADE;
DROP TABLE IF EXISTS leagues CASCADE;
DROP TABLE IF EXISTS matches CASCADE;
-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

CREATE TABLE leagues (
    league_id SERIAL PRIMARY KEY,
    league_name VARCHAR(100) NOT NULL UNIQUE,  -- e.g., 'Premier League'
    country VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    flag_url VARCHAR(255),
    logo_url VARCHAR(255),
    number_of_seasons INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_seasons (
    season_id SERIAL PRIMARY KEY,
    season_year INTEGER NOT NULL UNIQUE,  -- e.g., 2024
    season_name VARCHAR(10) NOT NULL UNIQUE,  -- e.g., '2024-25'
    league_id INTEGER REFERENCES leagues(league_id),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    is_current BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS dim_venues (
    venue_id INTEGER PRIMARY KEY,
    venue_name VARCHAR(100) NOT NULL,
    address TEXT,
    city VARCHAR(100),
    capacity INTEGER,
    surface VARCHAR(50),
    image_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_teams (
    team_id INTEGER PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL,
    short_name VARCHAR(50),
    team_code VARCHAR(10),
    country VARCHAR(50),
    founded_year INTEGER,
    is_national BOOLEAN DEFAULT FALSE,
    logo_url TEXT,
    venue_id INTEGER REFERENCES dim_venues(venue_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_teams_country ON dim_teams(country);

-- =============================================================================
-- RAW DATA STORAGE (for reprocessing)
-- =============================================================================

CREATE TABLE raw_api_responses (
    response_id SERIAL PRIMARY KEY,        -- ← We generate this (auto-increment)
    endpoint VARCHAR(100) NOT NULL,        -- ← We set this: '/leagues'
    request_params JSONB,                  -- ← We set this: {"id": "39"}
    response_data JSONB NOT NULL,          -- ← We store the ENTIRE API response here
    fetched_at TIMESTAMP DEFAULT NOW(),    -- ← Database sets this automatically
    created_at TIMESTAMP DEFAULT NOW()     -- ← Database sets this automatically
);

-- =============================================================================
-- RAW DATA STORAGE (for reprocessing)
-- =============================================================================


-- ============================================================================
-- FACT TABLE: Matches (Fixtures)
-- ============================================================================

CREATE TABLE matches (
    -- Primary Key
    fixture_id BIGINT PRIMARY KEY,
    
    -- Foreign Keys (Dimensions)
    league_id INTEGER NOT NULL REFERENCES leagues(league_id),
    season INTEGER NOT NULL REFERENCES dim_seasons(season_year),
    home_team_id INTEGER NOT NULL REFERENCES dim_teams(team_id),
    away_team_id INTEGER NOT NULL REFERENCES dim_teams(team_id),
    venue_id INTEGER REFERENCES dim_venues(venue_id) ,
    
    -- Match Metadata
    match_date TIMESTAMP NOT NULL,
    round VARCHAR(50),
    status VARCHAR(20) NOT NULL, -- 'FT', 'NS', 'LIVE', 'PST', 'CANC'
    status_long VARCHAR(100),
    referee VARCHAR(255),
    timezone VARCHAR(50),
    
    -- Score Information
    home_goals INTEGER,
    away_goals INTEGER,
    halftime_home_goals INTEGER,
    halftime_away_goals INTEGER,
    fulltime_home_goals INTEGER,
    fulltime_away_goals INTEGER,
    extratime_home_goals INTEGER,
    extratime_away_goals INTEGER,
    penalty_home_goals INTEGER,
    penalty_away_goals INTEGER,
    
    -- Match Result (derived)
    winner VARCHAR(10), -- 'HOME', 'AWAY', 'DRAW', NULL for unfinished
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT valid_status CHECK (status IN ('TBD', 'NS', '1H', 'HT', '2H', 'ET', 'P', 'FT', 'AET', 'PEN', 'BT', 'SUSP', 'INT', 'PST', 'CANC', 'ABD', 'AWD', 'WO', 'LIVE')),
    CONSTRAINT different_teams CHECK (home_team_id != away_team_id)
);

-- Indexes for performance
-- CREATE INDEX IF NOT EXISTS idx_matches_league_season ON matches(league_id, season);
-- CREATE INDEX IF NOT EXISTS idx_matches_home_team ON matches(home_team_id);
-- CREATE INDEX IF NOT EXISTS idx_matches_away_team ON matches(away_team_id);
-- CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date);
-- CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(status);
-- CREATE INDEX IF NOT EXISTS idx_matches_round ON matches(round);

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

-- ============================================================================
-- FACT TABLE: Player Match Stats
-- ============================================================================
CREATE TABLE IF NOT EXISTS fact_player_stats (
    stat_id SERIAL PRIMARY KEY,
    fixture_id BIGINT NOT NULL REFERENCES matches(fixture_id),
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
    passes_accuracy VARCHAR(10),
    
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
    penalty_commited INTEGER, -- typo in API response is often 'commited' or 'committed', standardizing here
    penalty_scored INTEGER,
    penalty_missed INTEGER,
    penalty_saved INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(fixture_id, player_id) 
);

CREATE INDEX idx_player_stats_fixture ON fact_player_stats(fixture_id);
CREATE INDEX idx_player_stats_player ON fact_player_stats(player_id);
CREATE INDEX idx_player_stats_team ON fact_player_stats(team_id);

-- ============================================================================
-- FACT TABLE: League Standings
-- ============================================================================
CREATE TABLE IF NOT EXISTS fact_standings (
    standing_id SERIAL PRIMARY KEY,
    league_id INTEGER NOT NULL REFERENCES leagues(league_id),
    season INTEGER NOT NULL REFERENCES dim_seasons(season_year),
    rank INTEGER NOT NULL,
    team_id INTEGER NOT NULL REFERENCES dim_teams(team_id),
    points INTEGER,
    goals_diff INTEGER,
    form VARCHAR(20),
    played INTEGER,
    win INTEGER,
    draw INTEGER,
    lose INTEGER,
    description VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(league_id, season, team_id)
);

CREATE INDEX idx_standings_league_season ON fact_standings(league_id, season);