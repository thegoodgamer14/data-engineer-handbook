-- Query 7: DDL for host_activity_reduced table
CREATE TABLE host_activity_reduced (
    month DATE,
    host STRING,
    hit_array ARRAY<BIGINT>,
    unique_visitors_array ARRAY<BIGINT>
)
USING DELTA;