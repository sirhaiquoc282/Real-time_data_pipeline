from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,  
    from_unixtime, 
    date_trunc,
    date_format,
    hour,
    dayofweek,
    dayofmonth,
    dayofyear,
    weekofyear,
    month,
    quarter,
    year,
    md5,
    concat,
    expr,
    lit,
    coalesce
)
# Custom UDFs for data extraction
from main.udfs.url_udfs import get_product_name
from main.udfs.ip_udfs import get_country, get_region, get_city
from main.udfs.user_agent_udfs import get_browser, get_os


def process_dim_time(raw_df: DataFrame) -> DataFrame:
    """
    Process time dimension table from raw event timestamps.
    
    Transforms Unix timestamps into a comprehensive time dimension with various
    time attributes for analytical queries (hourly, daily, weekly, monthly, quarterly).
    
    Args:
        raw_df (DataFrame): Raw DataFrame containing 'time_stamp' column in Unix format
        
    Returns:
        DataFrame: Time dimension table with the following schema:
            - time_id (int): Primary key in YYYYMMDDHH format
            - timestamp (timestamp): Hourly truncated timestamp
            - hour (int): Hour of day (0-23)
            - day_of_week (int): Day of week (1-7, Sunday=1)
            - is_weekend (bool): True if Saturday or Sunday
            - day_of_month (int): Day of month (1-31)
            - day_of_year (int): Day of year (1-366)
            - week_of_month (int): Week within month (1-5)
            - week_of_year (int): Week of year (1-53)
            - month_of_year (int): Month (1-12)
            - quarter (int): Quarter (1-4)
            - year (int): Year
    
    Business Rules:
        - Time grain is hourly (truncated to hour boundary)
        - Weekend definition: Saturday (7) and Sunday (1)
        - Week of month calculated using ceiling division by 7
    """
    return (
        raw_df
        # Convert Unix timestamp to readable datetime
        .withColumn("time_stamp", from_unixtime(col("time_stamp")))
        # Truncate to hourly granularity for consistent time dimension
        .withColumn("hourly_timestamp", date_trunc("hour", col("time_stamp")))
        # Create integer-based primary key for efficient joins
        .withColumn("time_id", date_format(col("hourly_timestamp"), "yyyyMMddHH").cast("integer"))
        # Extract time components for various analytical dimensions
        .withColumn("hour", hour(col("hourly_timestamp")))
        .withColumn("day_of_week", dayofweek(col("hourly_timestamp")))
        # Weekend flag for business analysis (Sunday=1, Saturday=7)
        .withColumn("is_weekend", col("day_of_week").isin([1, 7]))
        .withColumn("day_of_month", dayofmonth(col("hourly_timestamp")))
        .withColumn("day_of_year", dayofyear(col("hourly_timestamp")))
        # Calculate week of month using ceiling division
        .withColumn("week_of_month", expr("cast(ceil(day_of_month / 7.0) as int)"))
        .withColumn("week_of_year", weekofyear(col("hourly_timestamp")))
        .withColumn("month_of_year", month(col("hourly_timestamp")))
        .withColumn("quarter", quarter(col("hourly_timestamp")))
        .withColumn("year", year(col("hourly_timestamp")))
        # Select and alias columns for final dimension table
        .select(
            "time_id",
            col("hourly_timestamp").alias("timestamp"),
            "hour",
            "day_of_week",
            "is_weekend",
            "day_of_month",
            "day_of_year",
            "week_of_month",
            "week_of_year",
            "month_of_year",
            "quarter",
            "year"
        )
        # Ensure uniqueness on primary key
        .dropDuplicates(["time_id"])
    )


def process_dim_store(raw_df: DataFrame) -> DataFrame:
    """
    Process store dimension table from raw event data.
    
    Creates a simple store dimension with standardized naming convention.
    
    Args:
        raw_df (DataFrame): Raw DataFrame containing 'store_id' column
        
    Returns:
        DataFrame: Store dimension table with schema:
            - store_id (int): Primary key
            - store_name (str): Standardized store name with 'store_' prefix
            
    Business Rules:
        - Store names follow format: 'store_{store_id}'
        - Store IDs are cast to integer for consistency
    """
    return (
        raw_df
        # Create standardized store names with consistent prefix
        .withColumn("store_name", concat(lit("store_"), col("store_id")))
        .select(
            col("store_id").cast("integer"),  # Ensure integer type for joins
            "store_name"
        )
        # Remove duplicate stores
        .dropDuplicates(["store_id"])
    )


def add_missing_column(df: DataFrame, column_name: str, default_value):
    """
    Utility function to safely add missing columns to DataFrames.
    
    This handles schema differences between different event types, particularly
    between recommended and non-recommended product events.
    
    Args:
        df (DataFrame): Input DataFrame
        column_name (str): Name of column to add if missing
        default_value: Default value for the new column (typically None)
        
    Returns:
        DataFrame: DataFrame with the column added if it was missing, 
                  or unchanged if column already exists
    """
    if column_name not in df.columns:
        return df.withColumn(column_name, lit(default_value))
    return df


def process_dim_product(raw_df: DataFrame) -> DataFrame:
    """
    Process product dimension table with special handling for recommended products.
    
    Handles two types of product events:
    1. Regular product views (use product_id directly)
    2. Recommended product views (use viewing_product_id, fallback to product_id)
    
    Args:
        raw_df (DataFrame): Raw DataFrame containing product information
        
    Returns:
        DataFrame: Product dimension table with schema:
            - product_id (int): Primary key
            - product_name (str): Product name extracted from URL
            
    Business Rules:
        - Recommended products: identified by collection starting with 'product_detail_recommendation'
        - For recommended products: prefer viewing_product_id over product_id
        - Product names extracted from current_url using UDF
        - Union both product types for complete product catalog
    """
    # Separate recommended and non-recommended product events
    recommended_product_df = raw_df.filter(col("collection").startswith("product_detail_recommendation"))
    non_recommended_product_df = raw_df.filter(~col("collection").startswith("product_detail_recommendation"))

    # Handle schema differences between event types
    recommended_product_df = add_missing_column(non_recommended_product_df, "viewing_product_id", None)
    recommended_product_df = add_missing_column(recommended_product_df, "product_id", None) 
    non_recommended_product_df = add_missing_column(non_recommended_product_df, "product_id", None) 

    # Process recommended products with special ID logic
    recommended_product_df = (
        recommended_product_df
        # Extract product name from URL using custom UDF
        .withColumn("product_name", get_product_name(col("current_url")))
        # Use viewing_product_id if available, otherwise fallback to product_id
        .withColumn(
            "product_id",
            coalesce(col("viewing_product_id"), col("product_id")).cast("integer")
        )
        .select("product_id", "product_name")
    )
    
    # Process regular products
    non_recommended_product_df = (
        non_recommended_product_df
        .withColumn("product_name", get_product_name(col("current_url")))
        .select(
            col("product_id").cast("integer"),
            "product_name"
        )
    )

    # Combine both product types into unified dimension
    return (
        recommended_product_df.unionByName(non_recommended_product_df)
        .drop_duplicates(["product_id"])  # Ensure unique products
    )


def process_dim_location(raw_df: DataFrame) -> DataFrame:
    """
    Process location dimension table from IP addresses.
    
    Extracts geographic information using IP geolocation and creates
    a composite location dimension.
    
    Args:
        raw_df (DataFrame): Raw DataFrame containing 'ip' column
        
    Returns:
        DataFrame: Location dimension table with schema:
            - location_id (str): Primary key (MD5 hash of city+region+country)
            - city (str): City name
            - region (str): Region/state name  
            - country (str): Country name
            
    Business Rules:
        - Geographic data extracted from IP using custom UDFs
        - location_id is MD5 hash to handle composite geographic keys
        - Handles multiple IPs resolving to same geographic location
    """
    return (
        raw_df
        # Extract geographic components using IP geolocation UDFs
        .withColumn("city", get_city(col("ip")))
        .withColumn("region", get_region(col("ip")))
        .withColumn("country", get_country(col("ip")))
        # Create composite key as MD5 hash for consistent joins
        .withColumn("location_id", md5(concat(col("city"), col("region"), col("country"))))
        .select(
            "location_id",
            "city",
            "region",
            "country"
        )
        # Remove duplicate locations (multiple IPs may resolve to same location)
        .dropDuplicates(["location_id"])
    )


def process_dim_referrer_url(raw_df: DataFrame) -> DataFrame:
    """
    Process referrer URL dimension table.
    
    Creates dimension for tracking traffic sources and referral patterns.
    
    Args:
        raw_df (DataFrame): Raw DataFrame containing 'referrer_url' column
        
    Returns:
        DataFrame: Referrer URL dimension table with schema:
            - referrer_url_id (str): Primary key (MD5 hash of URL)
            - referrer_url (str): Full referrer URL
            
    Business Rules:
        - Uses MD5 hash for consistent, manageable key size
        - Preserves full URL for detailed analysis
    """
    return (
        raw_df
        # Create hash-based key for URL dimension
        .withColumn("referrer_url_id", md5(col("referrer_url")))
        .select(
            "referrer_url_id",
            "referrer_url"
        )
        # Remove duplicate URLs
        .dropDuplicates(["referrer_url_id"])
    )


def process_dim_browser(raw_df: DataFrame) -> DataFrame:
    """
    Process browser dimension table from user agent strings.
    
    Extracts browser information for user behavior analysis.
    
    Args:
        raw_df (DataFrame): Raw DataFrame containing 'user_agent' column
        
    Returns:
        DataFrame: Browser dimension table with schema:
            - browser_id (str): Primary key (MD5 hash of browser name)
            - browser (str): Browser name (e.g., Chrome, Firefox, Safari)
            
    Business Rules:
        - Browser extracted from user agent using custom UDF
        - Hash-based key for consistent joins
    """
    return (
        raw_df
        # Extract browser name from user agent string
        .withColumn("browser", get_browser(col("user_agent")))
        # Create hash-based key
        .withColumn("browser_id", md5(col("browser")))
        .select(
            "browser_id",
            "browser"
        )
        # Remove duplicate browsers
        .dropDuplicates(["browser_id"])
    )


def process_dim_os(raw_df: DataFrame) -> DataFrame:
    """
    Process operating system dimension table from user agent strings.
    
    Extracts OS information for device and platform analysis.
    
    Args:
        raw_df (DataFrame): Raw DataFrame containing 'user_agent' column
        
    Returns:
        DataFrame: OS dimension table with schema:
            - os_id (str): Primary key (MD5 hash of OS name)
            - os (str): Operating system name (e.g., Windows, macOS, Linux)
            
    Business Rules:
        - OS extracted from user agent using custom UDF  
        - Hash-based key for consistent joins
    """
    return (
        raw_df
        # Extract OS name from user agent string
        .withColumn("os", get_os(col("user_agent")))
        # Create hash-based key
        .withColumn("os_id", md5(col("os")))
        .select(
            "os_id",
            "os"
        )
        # Remove duplicate operating systems
        .dropDuplicates(["os_id"])
    )


def process_fact_view_event(raw_df: DataFrame) -> DataFrame:
    """
    Process fact table for view events with foreign keys to all dimensions.
    
    Creates the central fact table in the star schema, containing foreign keys
    to all dimension tables and representing the grain of one view event.
    
    Args:
        raw_df (DataFrame): Raw DataFrame containing all event data
        
    Returns:
        DataFrame: Fact table with schema:
            - event_id (str): Primary key (original event ID)
            - event_type (str): Type of event (collection name)
            - time_id (str): Foreign key to dim_time
            - product_id (int): Foreign key to dim_product  
            - store_id (int): Foreign key to dim_store
            - location_id (str): Foreign key to dim_location
            - referrer_url_id (str): Foreign key to dim_referrer_url
            - browser_id (str): Foreign key to dim_browser
            - os_id (str): Foreign key to dim_os
            
    Business Rules:
        - Same product ID logic as dim_product processing
        - All foreign keys generated using same logic as dimension tables
        - Handles both recommended and non-recommended product events
        - Grain: One row per product view event
    """
    # Separate event types for different processing logic
    recommended_product_df = raw_df.filter(col("collection").startswith("product_detail_recommendation"))
    non_recommended_product_df = raw_df.filter(~col("collection").startswith("product_detail_recommendation"))
    
    # Handle missing columns in both event types
    recommended_product_df = add_missing_column(recommended_product_df, "viewing_product_id", None)
    recommended_product_df = add_missing_column(recommended_product_df, "product_id", None)
    non_recommended_product_df = add_missing_column(non_recommended_product_df, "product_id", None)
    
    # Process recommended product events
    recommended_product_df = (
        recommended_product_df
        # Time dimension foreign key generation (matches dim_time logic)
        .withColumn("time_stamp", from_unixtime("time_stamp"))
        .withColumn("hourly_timestamp", date_trunc("hour", "time_stamp"))
        .withColumn("time_id", date_format("hourly_timestamp", "yyyyMMddHH"))
        
        # Location dimension foreign key generation (matches dim_location logic)
        .withColumn("city", get_city(col("ip")))
        .withColumn("region", get_region(col("ip")))
        .withColumn("country", get_country(col("ip")))
        .withColumn("location_id", md5(concat(col("city"), col("region"), col("country"))))
        
        # Other dimension foreign keys (match respective dimension logic)
        .withColumn("referrer_url_id", md5(col("referrer_url")))
        .withColumn("os", get_os(col("user_agent")))
        .withColumn("os_id", md5(col("os")))
        .withColumn("browser", get_browser(col("user_agent")))
        .withColumn("browser_id", md5(col("browser")))
        
        # Product ID logic (same as dim_product)
        .withColumn(
            "product_id",
            coalesce(col("viewing_product_id"), col("product_id")).cast("integer")
        )
        
        # Select fact table columns with appropriate aliases
        .select(
            col("id").alias("event_id"),           # Primary key
            col("collection").alias("event_type"), # Event classification
            "time_id",                             # FK to dim_time
            "product_id",                          # FK to dim_product
            "store_id",                            # FK to dim_store
            "location_id",                         # FK to dim_location
            "referrer_url_id",                     # FK to dim_referrer_url
            "browser_id",                          # FK to dim_browser
            "os_id"                               # FK to dim_os
        )
    )
    
    # Process non-recommended product events (similar logic, different product ID handling)
    non_recommended_product_df = (
        non_recommended_product_df
        # Time dimension processing
        .withColumn("time_stamp", from_unixtime("time_stamp"))
        .withColumn("hourly_timestamp", date_trunc("hour", "time_stamp"))
        .withColumn("time_id", date_format("hourly_timestamp", "yyyyMMddHH"))
        
        # Location dimension processing
        .withColumn("city", get_city(col("ip")))
        .withColumn("region", get_region(col("ip")))
        .withColumn("country", get_country(col("ip")))
        .withColumn("location_id", md5(concat(col("city"), col("region"), col("country"))))
        
        # Other dimensions
        .withColumn("referrer_url_id", md5(col("referrer_url")))
        .withColumn("os", get_os(col("user_agent")))
        .withColumn("os_id", md5(col("os")))
        .withColumn("browser", get_browser(col("user_agent")))
        .withColumn("browser_id", md5(col("browser")))
        
        # Select fact table columns
        .select(
            col("id").alias("event_id"),
            col("collection").alias("event_type"),
            "time_id",
            col("product_id").cast("integer"),     # Direct product_id for non-recommended
            "store_id",
            "location_id",
            "referrer_url_id",
            "browser_id",
            "os_id"
        )
    )
    
    # Combine both event types into unified fact table
    return (
        recommended_product_df.unionByName(non_recommended_product_df)
        .dropDuplicates(["product_id"])  # Ensure unique events (Note: This might need to be event_id)
    )