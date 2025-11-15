-- uuid тестовый
SELECT *
FROM devices
WHERE id = 'a1d75f50-5e4e-458a-a9cb-9698833b4c47';

SELECT *
FROM device_stats
WHERE device_id = 'a1d75f50-5e4e-458a-a9cb-9698833b4c47' FOR UPDATE;

SELECT *
FROM customers;

-- skip test user
SELECT *
FROM customers
Where id != '1fb3c4f6-bf73-4431-9935-d8e91f805385';

-- for changing of emails
SELECT email, notifications
from customer_contacts
where customer_id = 'customer_id';

UPDATE customer_contacts
SET email ='email'
where id = 'customer_id';

INSERT INTO customer_contacts
values ('id', 'customer_id', 'email', 'offline');

UPDATE customer_contacts
SET notifications='offline'
where id = 'customer_id';

SELECT email
from admins
where id = 'admin_id';

UPDATE admins
SET email='email'
where id = 'admin_id';

-- getting all countries
SELECT *
FROM countries
order by name;

SELECT *
FROM device_types;

SELECT *
FROM device_models;

-- check customer exists on insert
SELECT EXISTS (SELECT 1 FROM customers WHERE customer_number = 'abc');

INSERT INTO customers
values ('id', 'name', 'country_id', 'number');

SELECT *
FROM devices
where customer_id = 'customer_id';


SELECT *
FROM devices
WHERE customer_id = 'customer_id'
  AND device_type_id = 'device_type_id'
  AND device_model_id = 'device_model_id'
  AND serial_number = 'serial_number';

SELECT EXISTS (SELECT 1 FROM devices WHERE id = 'device_id');

SELECT id,
       device_id,
       file_name,
       upload_date,
       app_version,
       file_size,
       event_code,
       event_message,
       event_timestamp,
       plate_cycles,
       firmware_version,
       total_area
FROM documents
WHERE device_id = 'device_id'
ORDER BY upload_date DESC;

SELECT *
FROM documents
WHERE device_id = 'device_id';
