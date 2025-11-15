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
