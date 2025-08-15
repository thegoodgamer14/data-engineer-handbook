-- Query 2: DDL for user_devices_cumulated table
CREATE TABLE user_devices_cumulated (
    user_id BIGINT,
    device_activity_datelist MAP<STRING, ARRAY<DATE>>,
    date DATE
)
USING DELTA;