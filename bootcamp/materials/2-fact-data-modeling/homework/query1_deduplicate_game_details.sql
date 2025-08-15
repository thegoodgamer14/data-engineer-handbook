-- Query 1: Deduplicate game_details from Day 1
WITH day_1_games AS (
    SELECT game_id
    FROM games
    WHERE game_date_est = (SELECT MIN(game_date_est) FROM games)
)
SELECT DISTINCT gd.*
FROM game_details gd
JOIN day_1_games d1 ON gd.game_id = d1.game_id;