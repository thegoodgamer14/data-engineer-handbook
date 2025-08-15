-- Query 8: Incremental query that loads host_activity_reduced day-by-day
WITH yesterday AS (
    SELECT *
    FROM host_activity_reduced
    WHERE month = DATE_TRUNC('month', DATE('{{ ds }}'))
),
today_metrics AS (
    SELECT 
        host,
        COUNT(1) as daily_hits,
        COUNT(DISTINCT user_id) as daily_unique_visitors
    FROM events
    WHERE DATE(event_time) = DATE('{{ ds }}')
    AND host IS NOT NULL
    GROUP BY host
)
SELECT
    DATE_TRUNC('month', DATE('{{ ds }}')) as month,
    COALESCE(t.host, y.host) as host,
    CASE 
        WHEN y.hit_array IS NULL THEN array(t.daily_hits)
        WHEN t.host IS NULL THEN y.hit_array
        ELSE array_union(y.hit_array, array(t.daily_hits))
    END as hit_array,
    CASE 
        WHEN y.unique_visitors_array IS NULL THEN array(t.daily_unique_visitors)
        WHEN t.host IS NULL THEN y.unique_visitors_array
        ELSE array_union(y.unique_visitors_array, array(t.daily_unique_visitors))
    END as unique_visitors_array
FROM today_metrics t
FULL OUTER JOIN yesterday y ON t.host = y.host;