-- NFL Analytics Database Schema
-- This schema stores NFL game data for backtesting and analysis

-- Enable UUID extension for future use
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Main games table to store all NFL game data
CREATE TABLE games (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL UNIQUE,
    game_date DATE NOT NULL,
    away_team VARCHAR(50) NOT NULL,
    away_score INTEGER,
    home_team VARCHAR(50) NOT NULL,
    home_score INTEGER,
    
    -- Away team betting lines
    away_spread_open DECIMAL(4,1),
    away_spread_open_odds INTEGER,
    away_spread_close DECIMAL(4,1),
    away_spread_close_odds INTEGER,
    away_moneyline_open INTEGER,
    away_moneyline_close INTEGER,
    
    -- Home team betting lines
    home_spread_open DECIMAL(4,1),
    home_spread_open_odds INTEGER,
    home_spread_close DECIMAL(4,1),
    home_spread_close_odds INTEGER,
    home_moneyline_open INTEGER,
    home_moneyline_close INTEGER,
    
    -- Totals (Over/Under)
    over_open DECIMAL(4,1),
    over_open_odds INTEGER,
    over_close DECIMAL(4,1),
    over_close_odds INTEGER,
    under_open DECIMAL(4,1),
    under_open_odds INTEGER,
    under_close DECIMAL(4,1),
    under_close_odds INTEGER,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX idx_games_date ON games(game_date);
CREATE INDEX idx_games_away_team ON games(away_team);
CREATE INDEX idx_games_home_team ON games(home_team);
CREATE INDEX idx_games_created_at ON games(created_at);

-- Create a composite index for team-based queries
CREATE INDEX idx_games_teams_date ON games(away_team, home_team, game_date);

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to automatically update the updated_at column
CREATE TRIGGER update_games_updated_at 
    BEFORE UPDATE ON games 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Create a view for easier game result analysis
CREATE VIEW game_results AS
SELECT 
    game_id,
    game_date,
    away_team,
    away_score,
    home_team,
    home_score,
    CASE 
        WHEN away_score > home_score THEN away_team
        WHEN home_score > away_score THEN home_team
        ELSE 'TIE'
    END as winner,
    abs(away_score - home_score) as point_difference,
    (away_score + home_score) as total_points,
    CASE 
        WHEN (away_score + home_score) > over_close THEN 'OVER'
        WHEN (away_score + home_score) < over_close THEN 'UNDER'
        ELSE 'PUSH'
    END as total_result,
    -- Away team spread result
    CASE 
        WHEN (away_score + away_spread_close) > home_score THEN 'COVER'
        WHEN (away_score + away_spread_close) < home_score THEN 'NO_COVER'
        ELSE 'PUSH'
    END as away_spread_result,
    -- Home team spread result  
    CASE 
        WHEN (home_score + home_spread_close) > away_score THEN 'COVER'
        WHEN (home_score + home_spread_close) < away_score THEN 'NO_COVER'
        ELSE 'PUSH'
    END as home_spread_result
FROM games
WHERE away_score IS NOT NULL AND home_score IS NOT NULL;