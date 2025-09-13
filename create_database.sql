CREATE TABLE IF NOT EXISTS users(
    id CHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    role_id VARCHAR(255) NOT NULL,
    email_varchar(255) UNIQUE 
);

CREATE TABLE IF NOT EXISTS customer_data(
    id CHAR(36) PRIMARY KEY,
    user_id CHAR(36) NOT NULL UNIQUE,
    customer_number VARCHAR(255) NOT NULL,
    country_id CHAR(36) NOT NULL,
);

