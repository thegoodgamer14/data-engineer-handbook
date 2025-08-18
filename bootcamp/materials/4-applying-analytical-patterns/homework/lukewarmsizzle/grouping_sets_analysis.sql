-- GROUPING SETS Analysis for game_details data
-- This query uses GROUPING SETS to efficiently aggregate data along multiple dimensions:
-- 1. Player and Team - Who scored the most points playing for one team?
-- 2. Player and Season - Who scored the most points in one season?  
-- 3. Team - Which team has won the most games?

-- First, let's create a comprehensive aggregation using GROUPING SETS
WITH game_details_with_season AS (
    SELECT 
        gd.*,
        g.season,
        g.home_team_wins,
        CASE 
            WHEN gd.team_id = g.home_team_id AND g.home_team_wins = 1 THEN 1
            WHEN gd.team_id = g.visitor_team_id AND g.home_team_wins = 0 THEN 1  
            ELSE 0
        END as team_won
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
),
aggregated_stats AS (
    SELECT 
        player_name,
        team_abbreviation,
        season,
        SUM(pts) as total_points,
        SUM(team_won) as total_wins,
        COUNT(*) as games_played,
        AVG(pts) as avg_points_per_game
    FROM game_details_with_season
    GROUP BY GROUPING SETS (
        (player_name, team_abbreviation),  -- Player and Team dimension
        (player_name, season),             -- Player and Season dimension  
        (team_abbreviation)                -- Team dimension
    )
)

-- Answer: Who scored the most points playing for one team?
SELECT 
    'Player-Team: Most Points' as analysis_type,
    player_name,
    team_abbreviation,
    NULL as season,
    total_points,
    games_played,
    avg_points_per_game,
    NULL as total_wins
FROM aggregated_stats
WHERE player_name IS NOT NULL 
  AND team_abbreviation IS NOT NULL 
  AND season IS NULL
ORDER BY total_points DESC
LIMIT 10

UNION ALL

-- Answer: Who scored the most points in one season?
SELECT 
    'Player-Season: Most Points' as analysis_type,
    player_name,
    NULL as team_abbreviation,
    season,
    total_points,
    games_played,
    avg_points_per_game,
    NULL as total_wins
FROM aggregated_stats  
WHERE player_name IS NOT NULL
  AND team_abbreviation IS NULL
  AND season IS NOT NULL
ORDER BY total_points DESC
LIMIT 10

UNION ALL

-- Answer: Which team has won the most games?
SELECT 
    'Team: Most Wins' as analysis_type,
    NULL as player_name,
    team_abbreviation,
    NULL as season,
    NULL as total_points,
    games_played,
    NULL as avg_points_per_game,
    total_wins
FROM aggregated_stats
WHERE player_name IS NULL
  AND team_abbreviation IS NOT NULL  
  AND season IS NULL
ORDER BY total_wins DESC
LIMIT 10

ORDER BY analysis_type, total_points DESC, total_wins DESC;