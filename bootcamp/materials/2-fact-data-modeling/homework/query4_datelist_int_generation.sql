-- Query 4: Convert device_activity_datelist to datelist_int
SELECT 
    user_id,
    transform_values(
        device_activity_datelist,
        (k, v) -> transform(v, date_val -> CAST(date_format(date_val, 'yyyyMMdd') AS INT))
    ) as device_activity_datelist_int,
    date
FROM user_devices_cumulated;