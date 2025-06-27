\c glamira

CREATE TABLE IF NOT EXISTS dim_time (
    time_id BIGINT PRIMARY KEY,
    timestamp TIMESTAMP,
    hour INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    day_of_month INT,
    day_of_year INT,
    week_of_month INT,
    week_of_year INT,
    month_of_year INT,
    quarter INT,
    year INT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id VARCHAR PRIMARY KEY,
    city VARCHAR,
    region VARCHAR,
    country VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id INT PRIMARY KEY,
    store_name VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_referrer_url (
    referrer_url_id VARCHAR PRIMARY KEY,
    referrer_url TEXT
);

CREATE TABLE IF NOT EXISTS dim_browser (
    browser_id VARCHAR PRIMARY KEY,
    browser VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_os (
    os_id VARCHAR PRIMARY KEY,
    os VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_view_event (
    event_id VARCHAR PRIMARY KEY,
    event_type VARCHAR,
    time_id BIGINT,
    product_id INT,
    store_id INT,
    location_id VARCHAR,
    referrer_url_id VARCHAR,
    browser_id VARCHAR,
    os_id VARCHAR
);

CREATE TABLE IF NOT EXISTS canary_message (
    message_id UUID PRIMARY KEY,
    message TEXT,
    timestamp TIMESTAMP,
    type VARCHAR
);

