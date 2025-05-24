view_event_schema = {
    "date": """
        date_id CHAR PRIMARY KEY,
        full_date VARCHAR,
        day_of_week VARCHAR,
        day_of_week_short VARCHAR,
        is_weekday_or_weekend BOOLEAN,
        day_of_month BIGINT,
        year_month DATE,
        day_of_year BIGINT,
        week_of_year INT,
        quarter_number INT,
        year DATE,
        year_number INT
    """,
    
    "store": """
        store_id INT PRIMARY KEY,
        store_name VARCHAR
    """,

    "location": """
        location_id INT PRIMARY KEY,
        city_name VARCHAR,
        country_id INT,
        region_id INT
    """,

    "product": """
        product_id INT PRIMARY KEY,
        product_name VARCHAR
    """,

    "referrer_url": """
        referrer_url_id INT PRIMARY KEY,
        referrer_url TEXT
    """,

    "option": """
        option_id INT PRIMARY KEY,
        option_name VARCHAR
    """,

    "view_event": """
        event_id INT PRIMARY KEY,
        event_name VARCHAR,
        store_id INT REFERENCES store(store_id),
        date_id CHAR REFERENCES date(date_id),
        product_id INT REFERENCES product(product_id),
        location_id INT REFERENCES location(location_id),
        referrer_url_id INT REFERENCES referrer_url(referrer_url_id),
        option_id INT REFERENCES option(option_id),
        browser VARCHAR,
        os VARCHAR
    """
}
