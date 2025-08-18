-- SQL queries to answer the homework questions after running the Flink sessionization job

-- Question 1: What is the average number of web events of a session from a user on Tech Creator?
-- This query analyzes sessions from all Tech Creator hosts (zachwilson.techcreator.io and lulu.techcreator.io)

SELECT 
    'Tech Creator Average Events Per Session' as analysis_type,
    AVG(event_count) as avg_events_per_session,
    COUNT(*) as total_sessions,
    SUM(event_count) as total_events
FROM session_analysis 
WHERE host LIKE '%.techcreator.io';

-- Question 2: Compare results between different hosts
-- This query compares the average events per session across the three specified hosts

SELECT 
    host,
    AVG(event_count) as avg_events_per_session,
    COUNT(*) as total_sessions,
    SUM(event_count) as total_events,
    AVG(session_duration_minutes) as avg_session_duration_minutes
FROM session_analysis 
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host
ORDER BY avg_events_per_session DESC;

-- Alternative query using the pre-aggregated host comparison table
SELECT 
    host,
    avg_events_per_session,
    total_sessions,
    total_events
FROM host_comparison_analysis
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
ORDER BY avg_events_per_session DESC;

-- Detailed session analysis for Tech Creator domains
SELECT 
    host,
    ip,
    session_start,
    session_end,
    event_count,
    session_duration_minutes
FROM session_analysis 
WHERE host LIKE '%.techcreator.io'
ORDER BY host, session_start;

-- Summary statistics by host for all sessions
SELECT 
    host,
    COUNT(*) as session_count,
    AVG(event_count) as avg_events_per_session,
    MIN(event_count) as min_events_per_session,
    MAX(event_count) as max_events_per_session,
    AVG(session_duration_minutes) as avg_session_duration_minutes,
    COUNT(DISTINCT ip) as unique_users
FROM session_analysis 
GROUP BY host
HAVING COUNT(*) > 0
ORDER BY avg_events_per_session DESC;