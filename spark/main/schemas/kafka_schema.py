from pyspark.sql.types import StructType, StructField, StringType, IntegerType

view_event_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("api_version", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("current_url", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("local_time", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("time_stamp", IntegerType(), True),
    StructField("user_agent", StringType(), True),
    StructField("id", StringType(), True)
])