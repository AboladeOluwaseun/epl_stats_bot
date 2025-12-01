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

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

CREATE TABLE dim_seasons (
    season_id SERIAL PRIMARY KEY,
    season_name VARCHAR(10) NOT NULL UNIQUE,  -- e.g., '2024-25'
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    is_current BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


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