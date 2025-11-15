CREATE TABLE IF NOT EXISTS countries (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    code_2 VARCHAR(2) NOT NULL,
    code_3 VARCHAR(3) NOT NULL
);

CREATE TABLE IF NOT EXISTS admins(
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    country_id UUID REFERENCES countries (id)
);

CREATE TABLE IF NOT EXISTS device_types (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS device_models (
    id UUID PRIMARY KEY,
    device_type_id UUID NOT NULL REFERENCES device_types (id),
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
    id UUID PRIMARY KEY,
    device_model_id UUID NOT NULL REFERENCES device_models (id),
    serial_number VARCHAR(50) NOT NULL UNIQUE,
    equimpent_number VARCHAR(50) NOT NULL,
    suffix VARCHAR(50) NOT NULL,
    user_id UUID NOT NULL REFERENCES users (id),
    enabled BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS device_groups (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS user_device_group_relations (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL UNIQUE REFERENCES users (id),
    device_group_id UUID NOT NULL REFERENCES device_groups (id)
);

CREATE TABLE IF NOT EXISTS device_device_group_relations (
    id UUID PRIMARY KEY,
    device_id UUID NOT NULL REFERENCES devices (id),
    device_group_id UUID NOT NULL REFERENCES device_groups (id)
);

CREATE TABLE IF NOT EXISTS documents (
    id UUID PRIMARY KEY,
    device_id UUID NOT NULL REFERENCES devices (id),
    upload_date TIMESTAMP NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_content BYTEA NOT NULL,
    file_size INT NOT NULL,
    event_code VARCHAR(16),
    event_message VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS logs (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users (id),
    data TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users (id),
    device_id UUID REFERENCES devices (id),
    timestamp TIMESTAMP NOT NULL,
    is_sent BOOLEAN NOT NULL,
    content TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS device_stats (
    id UUID PRIMARY KEY,
    device_id UUID NOT NULL UNIQUE REFERENCES devices (id),
    active_since TIMESTAMP,
    last_upload_timestamp TIMESTAMP,
    total_file_count INT,
    total_sent_bytes BIGINT,
    max_file_size INT,
    firmware_version VARCHAR(32),
    windows_version VARCHAR(64),
    app_version VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS regions (
    id UUID PRIMARY KEY,
    name VARCHAR(255) not null
);


create type notification_type as enum ('all', 'offline', 'none');

CREATE TABLE customer_contacts
(
    id            UUID PRIMARY KEY,
    customer_id   UUID              NOT NULL,
    email         VARCHAR(255)      NOT NULL,
    notifications notification_type NOT NULL,
    CONSTRAINT customer_id_fk
        foreign key (customer_id)
            references customers (id)
            on delete cascade
)
