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