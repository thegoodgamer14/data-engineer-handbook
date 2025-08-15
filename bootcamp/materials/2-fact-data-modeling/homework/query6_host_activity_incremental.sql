-- Query 6: Incremental query to generate host_activity_datelist
WITH yesterday AS (
    SELECT *
    FROM hosts_cumulated
    WHERE date = DATE('{{ ds }}') - INTERVAL '1' DAY
),
today AS (
    SELECT 
        host,
        DATE(event_time) as event_date
    FROM events
    WHERE DATE(event_time) = DATE('{{ ds }}')
    AND host IS NOT NULL
    GROUP BY host, DATE(event_time)
)
SELECT
    COALESCE(t.host, y.host) as host,
    CASE 
        WHEN y.host_activity_datelist IS NULL THEN array(t.event_date)
        WHEN t.host IS NULL THEN y.host_activity_datelist
        ELSE array_union(y.host_activity_datelist, array(t.event_date))
    END as host_activity_datelist,
    DATE('{{ ds }}') as date
FROM today t
FULL OUTER JOIN yesterday y ON t.host = y.host;