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
from main.udfs.url_udfs import get_product_name
from main.udfs.ip_udfs import get_country, get_region, get_city
from main.udfs.user_agent_udfs import get_browser, get_os


def process_dim_time(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .withColumn("time_stamp", from_unixtime(col("time_stamp")))
        .withColumn("hourly_timestamp", date_trunc("hour", col("time_stamp")))
        .withColumn("time_id", date_format(col("hourly_timestamp"), "yyyyMMddHH").cast("integer"))
        .withColumn("hour", hour(col("hourly_timestamp")))
        .withColumn("day_of_week", dayofweek(col("hourly_timestamp")))
        .withColumn("is_weekend", col("day_of_week").isin([1, 7]))
        .withColumn("day_of_month", dayofmonth(col("hourly_timestamp")))
        .withColumn("day_of_year", dayofyear(col("hourly_timestamp")))
        .withColumn("week_of_month", expr("cast(ceil(day_of_month / 7.0) as int)"))
        .withColumn("week_of_year", weekofyear(col("hourly_timestamp")))
        .withColumn("month_of_year", month(col("hourly_timestamp")))
        .withColumn("quarter", quarter(col("hourly_timestamp")))
        .withColumn("year", year(col("hourly_timestamp")))
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
        .dropDuplicates(["time_id"])
    )
    
def process_dim_store(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .withColumn("store_name", concat(lit("store_"), col("store_id")))
        .select(
            col("store_id").cast("integer"),
            "store_name"
        )
        .dropDuplicates(["store_id"])
    )

def add_missing_column(df: DataFrame, column_name: str, default_value):
    if column_name not in df.columns:
        return df.withColumn(column_name, lit(default_value))
    return df

def process_dim_product(raw_df: DataFrame) -> DataFrame:
    recommended_product_df = raw_df.filter(col("collection").startswith("product_detail_recommendation"))
    non_recommended_product_df = raw_df.filter(~col("collection").startswith("product_detail_recommendation"))

    recommended_product_df = add_missing_column(non_recommended_product_df, "viewing_product_id", None)
    recommended_product_df = add_missing_column(recommended_product_df, "product_id", None) 

    non_recommended_product_df = add_missing_column(non_recommended_product_df, "product_id", None)

    recommended_product_df = (
        recommended_product_df
        .withColumn("product_name", get_product_name(col("current_url")))
        .withColumn(
            "product_id",
            coalesce(col("viewing_product_id"), col("product_id")).cast("integer")
        )
        .select("product_id", "product_name")
    )
    
    non_recommended_product_df = (
        non_recommended_product_df
        .withColumn("product_name", get_product_name(col("current_url")))
        .select(
            col("product_id").cast("integer"),
            "product_name"
        )
    )

    return (
        recommended_product_df.unionByName(non_recommended_product_df)
        .drop_duplicates(["product_id"])
    )


def process_dim_location(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .withColumn("city", get_city(col("ip")))
        .withColumn("region", get_region(col("ip")))
        .withColumn("country", get_country(col("ip")))
        .withColumn("location_id", md5(concat(col("city"), col("region"), col("country"))))
        .select(
            "location_id",
            "city",
            "region",
            "country"
        )
        .dropDuplicates(["location_id"])
    )


def process_dim_referrer_url(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .withColumn("referrer_url_id", md5(col("referrer_url")))
        .select(
            "referrer_url_id",
            "referrer_url"
        )
        .dropDuplicates(["referrer_url_id"])
    )


def process_dim_browser(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .withColumn("browser", get_browser(col("user_agent")))
        .withColumn("browser_id", md5(col("browser")))
        .select(
            "browser_id",
            "browser"
        )
        .dropDuplicates(["browser_id"])
    )


def process_dim_os(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .withColumn("os", get_os(col("user_agent")))
        .withColumn("os_id", md5(col("os")))
        .select(
            "os_id",
            "os"
        )
        .dropDuplicates(["os_id"])
    )


def process_fact_view_event(raw_df: DataFrame) -> DataFrame:
    recommended_product_df = raw_df.filter(col("collection").startswith("product_detail_recommendation"))
    non_recommended_product_df = raw_df.filter(~col("collection").startswith("product_detail_recommendation"))
    
    recommended_product_df = add_missing_column(recommended_product_df, "viewing_product_id", None)
    recommended_product_df = add_missing_column(recommended_product_df, "product_id", None)
    non_recommended_product_df = add_missing_column(non_recommended_product_df, "product_id", None)
    
    recommended_product_df = (
        recommended_product_df
        .withColumn("time_stamp", from_unixtime("time_stamp"))
        .withColumn("hourly_timestamp", date_trunc("hour", "time_stamp"))
        .withColumn("time_id", date_format("hourly_timestamp", "yyyyMMddHH"))
        .withColumn("city", get_city(col("ip")))
        .withColumn("region", get_region(col("ip")))
        .withColumn("country", get_country(col("ip")))
        .withColumn("location_id", md5(concat(col("city"), col("region"), col("country"))))
        .withColumn("referrer_url_id", md5(col("referrer_url")))
        .withColumn("os", get_os(col("user_agent")))
        .withColumn("os_id", md5(col("os")))
        .withColumn("browser", get_browser(col("user_agent")))
        .withColumn("browser_id", md5(col("browser")))
        .withColumn(
            "product_id",
            coalesce(col("viewing_product_id"), col("product_id")).cast("integer")
        )
        .select(
            col("id").alias("event_id"),
            col("collection").alias("event_type"),
            "time_id",
            "product_id",
            "store_id",
            "location_id",
            "referrer_url_id",
            "browser_id",
            "os_id"
        )
    )
    
    non_recommended_product_df = (
        non_recommended_product_df
        .withColumn("time_stamp", from_unixtime("time_stamp"))
        .withColumn("hourly_timestamp", date_trunc("hour", "time_stamp"))
        .withColumn("time_id", date_format("hourly_timestamp", "yyyyMMddHH"))
        .withColumn("city", get_city(col("ip")))
        .withColumn("region", get_region(col("ip")))
        .withColumn("country", get_country(col("ip")))
        .withColumn("location_id", md5(concat(col("city"), col("region"), col("country"))))
        .withColumn("referrer_url_id", md5(col("referrer_url")))
        .withColumn("os", get_os(col("user_agent")))
        .withColumn("os_id", md5(col("os")))
        .withColumn("browser", get_browser(col("user_agent")))
        .withColumn("browser_id", md5(col("browser")))
        .select(
            col("id").alias("event_id"),
            col("collection").alias("event_type"),
            "time_id",
            col("product_id").cast("integer"),
            "store_id",
            "location_id",
            "referrer_url_id",
            "browser_id",
            "os_id"
        )
    )
    
    return (
        recommended_product_df.unionByName(non_recommended_product_df)
        .dropDuplicates(["product_id"])
    )
