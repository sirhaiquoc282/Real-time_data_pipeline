from pyspark.sql.functions import col, date_format, dayofmonth, trunc, dayofyear, weekofyear, quarter, year, when, sha2, concat, hash, lit
from ..udfs.udfs import extract_country_code, extract_country_name, extract_product_name, extract_browser, extract_os


class DataProcessor:
    
    def process_dim_date(self,batch_df):
        """_summary_

        Args:
            batch_df (_type_): _description_

        Returns:
            _type_: _description_
        """
        dim_date = batch_df.select(
            date_format(col("local_time"), "yyyyMMdd").alias("date_id"),
            col("local_time").alias("full_date"),
            date_format(col("local_time"), "EEEE").alias("day_of_week"),
            date_format(col("local_time"), "E").alias("day_of_week_short"),
            when(date_format(col("local_time"), "E").isin("Sat", "Sun"), False)
            .otherwise(True).alias("is_weekday_or_weekend"),
            dayofmonth(col("local_time")).alias("day_of_month"),
            trunc(col("local_time"), "MM").alias("year_month"),
            dayofyear(col("local_time")).alias("day_of_year"),
            weekofyear(col("local_time")).alias("week_of_year"),
            quarter(col("local_time")).alias("quarter_number"),
            year(col("local_time")).alias("year_number")
        ).distinct()
        return dim_date


    def process_dim_product(self,batch_df):
        """_summary_

        Args:
            batch_df (_type_): _description_

        Returns:
            _type_: _description_
        """
        dim_product = batch_df.select(
            hash(extract_product_name(col("current_url")),256).alias("product_id"),
            extract_product_name(col("current_url")).alias("product_name")
        ).distinct()
        return dim_product

    def process_dim_store(self,batch_df):
        """_summary_

        Args:
            batch_df (_type_): _description_

        Returns:
            _type_: _description_
        """
        dim_store = batch_df.select(
            col("store_id").alias("store_id"),
            concat(lit("glamira_store_"), col("store_id")).alias("store_name"),
        ).distinct()
        return dim_store


    def process_dim_option(self,batch_df):
        pass


    def process_dim_location(self,batch_df):
        """_summary_

        Args:
            batch_df (_type_): _description_

        Returns:
            _type_: _description_
        """
        dim_location = batch_df.select(
            extract_country_code(col("current_url")).alias("country_code"),
            extract_country_name(col("current_url")).alias("country_name")
        ).distinct()
        return dim_location

    def process_fact_view_event(self,batch_df):
        """_summary_

        Args:
            batch_df (_type_): _description_

        Returns:
            _type_: _description_
        """
        fact_view_event = batch_df.select(
            col("id").alias("event_id"),
            col("api_version").alias("api_version"),
            col("collection").alias("collection"),
            col("store_id").alias("store_id"),
            date_format(col("local_time"), "yyyyMMdd").alias("date_id"),
            hash(extract_product_name(col("current_url")),256).alias("product_id"),
            extract_country_code(col("current_url")).alias("country_code"),
            col("referrer_url").alias("referrer_url"),
            extract_browser(col("user_agent")).alias("browser"),
            extract_os(col("user_agent")).alias("os")
        ).distinct()
        return fact_view_event


