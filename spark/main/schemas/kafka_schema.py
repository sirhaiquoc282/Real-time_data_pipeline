from pyspark.sql.types import StructType, StructField, StringType, IntegerType

"""
Kafka Schema Documentation - Product Views Topic

Schema definition for product view events consumed from the 'product_views' Kafka topic.
This schema defines the structure of data read from Kafka topics containing product view events.

Field Descriptions:
- _id: Unique identifier for the event record
- api_version: Version of the API that generated the event
- collection: Collection or dataset name
- current_url: URL of the page where the product view occurred
- device_id: Unique identifier for the user's device
- email: User's email address (if available)
- ip: IP address of the user
- local_time: Local timestamp when the event occurred (string format)
- product_id: Unique identifier of the viewed product
- referrer_url: URL of the referring page
- store_id: Identifier of the store/shop
- time_stamp: Unix timestamp of the event (integer)
- user_agent: Browser/client user agent string
- id: Additional identifier field
"""

view_event_schema = StructType([
    StructField("_id", StringType(), True),           # Unique event record ID
    StructField("api_version", StringType(), True),   # API version
    StructField("collection", StringType(), True),    # Collection/dataset name
    StructField("current_url", StringType(), True),   # Current page URL
    StructField("device_id", StringType(), True),     # Device identifier
    StructField("email", StringType(), True),         # User email
    StructField("ip", StringType(), True),            # User IP address
    StructField("local_time", StringType(), True),    # Local timestamp (string)
    StructField("product_id", StringType(), True),    # Product identifier
    StructField("referrer_url", StringType(), True),  # Referring page URL
    StructField("store_id", StringType(), True),      # Store identifier
    StructField("time_stamp", IntegerType(), True),   # Unix timestamp (int)
    StructField("user_agent", StringType(), True),    # Browser user agent
    StructField("id", StringType(), True)             # Additional ID field
])