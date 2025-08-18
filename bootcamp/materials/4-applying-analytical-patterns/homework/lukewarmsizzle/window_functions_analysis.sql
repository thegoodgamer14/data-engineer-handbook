-- Window Functions Analysis for game_details
-- This query uses window functions to answer:
-- 1. What is the most games a team has won in a 90 game stretch?
-- 2. How many games in a row did LeBron James score over 10 points a game?

-- Query 1: Most games a team has won in a 90 game stretch
WITH team_games_with_wins AS (
    SELECT 
        gd.team_id,
        gd.team_abbreviation,
        g.game_date_est,
        g.game_id,
        CASE 
            WHEN gd.team_id = g.home_team_id AND g.home_team_wins = 1 THEN 1
            WHEN gd.team_id = g.visitor_team_id AND g.home_team_wins = 0 THEN 1  
            ELSE 0
        END as team_won,
        ROW_NUMBER() OVER (PARTITION BY gd.team_id ORDER BY g.game_date_est) as game_number
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    WHERE gd.team_id IS NOT NULL
),
team_games_distinct AS (
    SELECT DISTINCT 
        team_id,
        team_abbreviation, 
        game_date_est,
        game_id,
        team_won,
        game_number
    FROM team_games_with_wins
),
rolling_wins AS (
    SELECT 
        team_id,
        team_abbreviation,
        game_date_est,
        game_number,
        team_won,
        SUM(team_won) OVER (
            PARTITION BY team_id 
            ORDER BY game_number 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as wins_in_90_games,
        COUNT(*) OVER (
            PARTITION BY team_id 
            ORDER BY game_number 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as games_in_window
    FROM team_games_distinct
),
max_wins_90_games AS (
    SELECT 
        team_abbreviation,
        MAX(wins_in_90_games) as max_wins_in_90_games
    FROM rolling_wins
    WHERE games_in_window = 90  -- Only consider complete 90-game windows
    GROUP BY team_id, team_abbreviation
)

-- Result 1: Most games won in 90 game stretch
SELECT 
    'Most Wins in 90 Games' as analysis_type,
    team_abbreviation,
    max_wins_in_90_games,
    NULL as player_name,
    NULL as consecutive_games_over_10pts
FROM max_wins_90_games
ORDER BY max_wins_in_90_games DESC
LIMIT 5

UNION ALL

-- Query 2: LeBron James consecutive games scoring over 10 points
WITH lebron_games AS (
    SELECT 
        gd.player_name,
        g.game_date_est,
        gd.pts,
        CASE WHEN gd.pts > 10 THEN 1 ELSE 0 END as scored_over_10,
        ROW_NUMBER() OVER (ORDER BY g.game_date_est) as game_sequence
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id  
    WHERE gd.player_name = 'LeBron James'
      AND gd.pts IS NOT NULL
    ORDER BY g.game_date_est
),
lebron_streaks AS (
    SELECT 
        player_name,
        game_date_est,
        pts,
        scored_over_10,
        game_sequence,
        game_sequence - ROW_NUMBER() OVER (ORDER BY game_sequence) as streak_group
    FROM lebron_games
    WHERE scored_over_10 = 1  -- Only games where he scored over 10
),
consecutive_games_over_10 AS (
    SELECT 
        player_name,
        streak_group,
        COUNT(*) as consecutive_games,
        MIN(game_date_est) as streak_start,
        MAX(game_date_est) as streak_end
    FROM lebron_streaks
    GROUP BY player_name, streak_group
)

-- Result 2: LeBron's longest consecutive games over 10 points
SELECT 
    'LeBron Consecutive 10+ Points' as analysis_type,
    NULL as team_abbreviation,
    NULL as max_wins_in_90_games,
    player_name,
    MAX(consecutive_games) as consecutive_games_over_10pts
FROM consecutive_games_over_10
GROUP BY player_name

ORDER BY analysis_type, max_wins_in_90_games DESC, consecutive_games_over_10pts DESC;