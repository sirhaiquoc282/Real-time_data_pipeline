from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

def get_kafka_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("api_version", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("current_url", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("local_time", StringType(), True),
        StructField("option", ArrayType(
            StructType([
                StructField("option_label", StringType(), True),
                StructField("option_id", StringType(), True),
                StructField("value_label", StringType(), True),
                StructField("value_id", StringType(), True),
                StructField("quality", StringType(), True),
                StructField("quality_label", StringType(), True),
                StructField("alloy", StringType(), True),
                StructField("diamond", StringType(), True),
                StructField("shapediamond", StringType(), True),
                StructField("stone", StringType(), True),
                StructField("pearlcolor", StringType(), True),
                StructField("finish", StringType(), True),
                StructField("price", StringType(), True),
                StructField("category_id", StringType(), True),
                StructField("category id", StringType(), True),
                StructField("kollektion", StringType(), True),
                StructField("kollektion_id", StringType(), True)
            ])
        ), True),
        StructField("product_id", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("time_stamp", IntegerType(), True),
        StructField("user_agent", StringType(), True)
    ])


