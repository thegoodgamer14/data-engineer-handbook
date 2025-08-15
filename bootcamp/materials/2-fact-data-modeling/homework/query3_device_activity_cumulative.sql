-- Query 3: Cumulative query to generate device_activity_datelist from events
WITH yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = DATE('{{ ds }}') - INTERVAL '1' DAY
),
today AS (
    SELECT 
        user_id,
        browser_type,
        DATE(event_time) as event_date
    FROM events
    WHERE DATE(event_time) = DATE('{{ ds }}')
    AND user_id IS NOT NULL
)
SELECT
    COALESCE(t.user_id, y.user_id) as user_id,
    CASE 
        WHEN y.device_activity_datelist IS NULL THEN 
            map(t.browser_type, array(t.event_date))
        WHEN t.user_id IS NULL THEN y.device_activity_datelist
        ELSE 
            map_concat(
                y.device_activity_datelist,
                map(t.browser_type, 
                    CASE 
                        WHEN y.device_activity_datelist[t.browser_type] IS NULL THEN array(t.event_date)
                        ELSE array_union(y.device_activity_datelist[t.browser_type], array(t.event_date))
                    END
                )
            )
    END as device_activity_datelist,
    DATE('{{ ds }}') as date
FROM today t
FULL OUTER JOIN yesterday y ON t.user_id = y.user_id;