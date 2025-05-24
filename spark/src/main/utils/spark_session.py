from pyspark.sql import SparkSession

def get_spark_session(app_name: str, master: str = "local[*]") -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()
    return spark

