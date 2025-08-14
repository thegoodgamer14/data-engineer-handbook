WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_flag = TRUE
),
this_year_data AS (
    SELECT 
        actor,
        actorid,
        quality_class,
        is_active
    FROM actors
    WHERE year = {{ current_year }}
),
scd_updates AS (
    SELECT 
        lys.actor,
        lys.actorid,
        lys.quality_class,
        lys.is_active,
        lys.start_date,
        CASE 
            WHEN lys.quality_class = tyd.quality_class AND lys.is_active = tyd.is_active 
            THEN {{ current_year }}
            ELSE {{ current_year }} - 1
        END as end_date,
        FALSE as current_flag,
        'UPDATE' as action_type
    FROM last_year_scd lys
    JOIN this_year_data tyd ON lys.actorid = tyd.actorid
    
    UNION ALL
    
    SELECT 
        tyd.actor,
        tyd.actorid,
        tyd.quality_class,
        tyd.is_active,
        {{ current_year }} as start_date,
        9999 as end_date,
        TRUE as current_flag,
        'INSERT' as action_type
    FROM this_year_data tyd
    LEFT JOIN last_year_scd lys ON tyd.actorid = lys.actorid
    WHERE lys.actorid IS NULL
       OR lys.quality_class != tyd.quality_class 
       OR lys.is_active != tyd.is_active
       
    UNION ALL
    
    SELECT 
        lys.actor,
        lys.actorid,
        lys.quality_class,
        lys.is_active,
        lys.start_date,
        lys.end_date,
        lys.current_flag,
        'KEEP' as action_type
    FROM last_year_scd lys
    LEFT JOIN this_year_data tyd ON lys.actorid = tyd.actorid
    WHERE tyd.actorid IS NULL
)

MERGE INTO actors_history_scd AS target
USING scd_updates AS source
ON target.actorid = source.actorid AND target.current_flag = TRUE
WHEN MATCHED AND source.action_type = 'UPDATE' THEN
    UPDATE SET 
        end_date = source.end_date,
        current_flag = source.current_flag
WHEN NOT MATCHED AND source.action_type IN ('INSERT', 'KEEP') THEN
    INSERT (actor, actorid, quality_class, is_active, start_date, end_date, current_flag)
    VALUES (source.actor, source.actorid, source.quality_class, source.is_active, 
            source.start_date, source.end_date, source.current_flag);