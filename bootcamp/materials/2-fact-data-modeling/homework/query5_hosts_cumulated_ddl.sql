-- Query 5: DDL for hosts_cumulated table
CREATE TABLE hosts_cumulated (
    host STRING,
    host_activity_datelist ARRAY<DATE>,
    date DATE
)
USING DELTA;