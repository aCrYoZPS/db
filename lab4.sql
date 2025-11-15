SELECT devices.*, device_stats.*, customers.*, device_models.*
FROM devices
         LEFT OUTER JOIN device_stats ON device_stats.device_id = devices.id
         LEFT OUTER JOIN customers ON customers.id = devices.customer_id
         LEFT OUTER JOIN device_models ON device_models.id = devices.device_model_id
WHERE device_stats.active_since IS NOT NULL
  AND customers.id != 'b4df387a-086b-47c7-a4a1-5d2f60202ad6';

SELECT devices.*, device_models.*
FROM devices
         LEFT OUTER JOIN device_stats ON public.device_stats.device_id = devices.id
         LEFT OUTER JOIN device_models ON device_models.id = devices.device_model_id
WHERE device_stats.active_since IS NULL
  AND customer_id != 'b4df387a-086b-47c7-a4a1-5d2f60202ad6';

SELECT device_models.*
FROM device_models
         JOIN device_types ON device_models.device_type_id = device_types.id
WHERE device_types.name = 'Avalon'
ORDER BY device_models.name;

SELECT devices.*, device_stats.*
FROM devices
         LEFT OUTER JOIN device_stats ON device_stats.device_id = devices.id
WHERE device_stats.active_since IS NOT NULL
  AND (
    (device_stats.last_upload_timestamp IS NULL AND device_stats.active_since <= '2025-11-08')
        OR (device_stats.last_upload_timestamp <= '2025-11-08')
    )
  AND (
    device_stats.last_offline_notification IS NULL
        OR device_stats.last_offline_notification <= '2025-11-08'
    );


WITH subquery AS (
    SELECT
        notification,
        changed_on,
        time_of_change,
        ROW_NUMBER() OVER (
            PARTITION BY notification
            ORDER BY changed_on DESC
        ) AS rownum
    FROM service_xpress_data
    WHERE notification IN (<list_of_notifications>)
)
SELECT
    subquery.notification,
    subquery.changed_on,
    subquery.time_of_change
FROM subquery
WHERE subquery.rownum = 1;


SELECT client_reports.*, device_stats.*
FROM client_reports
LEFT OUTER JOIN device_stats on device_stats.device_id = client_reports.device_id
WHERE client_reports.email_sent = false;
