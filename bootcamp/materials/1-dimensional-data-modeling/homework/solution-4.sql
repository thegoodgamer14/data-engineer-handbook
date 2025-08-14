INSERT INTO actors_history_scd
WITH actor_changes AS (
    SELECT 
        actor,
        actorid,
        year,
        quality_class,
        is_active,
        LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY year) as prev_quality_class,
        LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY year) as prev_is_active,
        LEAD(year, 1) OVER (PARTITION BY actorid ORDER BY year) as next_year
    FROM actors
),
scd_records AS (
    SELECT 
        actor,
        actorid,
        quality_class,
        is_active,
        year as start_date,
        COALESCE(next_year - 1, 9999) as end_date,
        CASE WHEN next_year IS NULL THEN TRUE ELSE FALSE END as current_flag
    FROM actor_changes
    WHERE prev_quality_class IS NULL 
       OR prev_quality_class != quality_class 
       OR prev_is_active != is_active
       OR (prev_is_active IS NULL AND is_active IS NOT NULL)
)
SELECT * FROM scd_records;