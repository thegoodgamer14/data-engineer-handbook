-- State Change Tracking for Players
-- This query tracks player state changes across seasons:
-- - New: Player entering the league
-- - Retired: Player leaving the league  
-- - Continued Playing: Player staying in the league
-- - Returned from Retirement: Player coming out of retirement
-- - Stayed Retired: Player staying out of the league

WITH player_states AS (
    SELECT 
        player_name,
        current_season,
        is_active,
        LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as prev_is_active,
        LAG(current_season, 1) OVER (PARTITION BY player_name ORDER BY current_season) as prev_season
    FROM players
    ORDER BY player_name, current_season
),
state_changes AS (
    SELECT 
        player_name,
        current_season,
        is_active,
        prev_is_active,
        prev_season,
        CASE 
            -- New player entering the league
            WHEN prev_is_active IS NULL AND is_active = TRUE THEN 'New'
            
            -- Player continuing to play
            WHEN prev_is_active = TRUE AND is_active = TRUE THEN 'Continued Playing'
            
            -- Player retiring
            WHEN prev_is_active = TRUE AND is_active = FALSE THEN 'Retired'
            
            -- Player returning from retirement
            WHEN prev_is_active = FALSE AND is_active = TRUE THEN 'Returned from Retirement'
            
            -- Player staying retired
            WHEN prev_is_active = FALSE AND is_active = FALSE THEN 'Stayed Retired'
            
            -- First record and inactive (edge case)
            WHEN prev_is_active IS NULL AND is_active = FALSE THEN 'New'
            
            ELSE 'Unknown'
        END as state_change
    FROM player_states
)
SELECT 
    player_name,
    current_season,
    is_active,
    state_change
FROM state_changes
ORDER BY player_name, current_season;