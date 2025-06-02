import logging
import argparse
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql.functions import col, from_json
from main.utils.spark_session import create_spark_session
from main.schemas.kafka_schema import view_event_schema
from main.processors.data_processors import (
    process_dim_time,
    process_dim_product,
    process_dim_location,
    process_dim_store,
    process_dim_referrer_url,
    process_dim_browser,
    process_dim_os,
    process_fact_view_event
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(module)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)



    

def parse_arguments():
    parser = argparse.ArgumentParser(description="Kafka to PostgreSQL Streaming Job")
    parser.add_argument("--bootstrap_servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--security_protocol", required=True, help="Security protocol (e.g. SASL_SSL)")
    parser.add_argument("--sasl_mechanism", required=True, help="SASL mechanism (e.g. PLAIN)")
    parser.add_argument("--sasl_plain_username", required=True, help="SASL username")
    parser.add_argument("--sasl_plain_password", required=True, help="SASL password")
    parser.add_argument("--kafka_topic", required=True, help="Kafka topic to consume from")
    parser.add_argument("--postgres_host", required=True, help="PostgreSQL host")
    parser.add_argument("--postgres_port", required=True, type=int, help="PostgreSQL port")
    parser.add_argument("--postgres_dbname", required=True, help="PostgreSQL database name")
    parser.add_argument("--postgres_user", required=True, help="PostgreSQL username")
    parser.add_argument("--postgres_password", required=True, help="PostgreSQL password")
    parser.add_argument("--checkpoint_location", required=True, help="Checkpoint location for Spark")
    parser.add_argument("--allowed_collections", nargs="+", default=["view_product_detail", 
                                                                    "select_product_option", 
                                                                    "select_product_option_quality", 
                                                                    "product_detail_recommendation_visible", 
                                                                    "product_detail_recommendation_noticed", 
                                                                    "product_detail_recommendation_clicked"], help="List of allowed collections to filter events")
    return parser.parse_args()


def write_to_postgres(df: DataFrame, epoch_id: int, table_name: str, args):
    import psycopg2
    from psycopg2.extras import execute_values
    conn = None
    try:
        logger.info(f"Processing batch {epoch_id} for table {table_name}")
        
        rows = df.collect()
        conn = psycopg2.connect(
            host=args.postgres_host,
            port=args.postgres_port,
            dbname=args.postgres_dbname,
            user=args.postgres_user,
            password=args.postgres_password
        )
        conn.autocommit = False
        cursor = conn.cursor()

        columns = df.columns
        data = [tuple(row) for row in rows]
        if table_name == "fact_view_event":
            conflict_column = "event_id"
        else:
            conflict_column = "_".join(table_name.split("_")[1::])+"_id"        
        
        execute_values(
            cursor,
            f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s ON CONFLICT ({conflict_column}) DO NOTHING",
            data
        )

        conn.commit()
        logger.info(f"Successfully wrote batch {epoch_id} to {table_name}")

    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

                    
def create_query(df: DataFrame, table_name: str, args):
    return (df.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, table_name, args))
        .option("checkpointLocation", f"{args.checkpoint_location}/{table_name}")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start())
            
            
def process(spark: SparkSession, args):
    kafka_options = {
        "kafka.bootstrap.servers": args.bootstrap_servers,
        "kafka.security.protocol": args.security_protocol,
        "kafka.sasl.mechanism": args.sasl_mechanism,
        "kafka.sasl.jaas.config": 
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username=\"{args.sasl_plain_username}\" "
            f"password=\"{args.sasl_plain_password}\";",
        "subscribe": args.kafka_topic,
        "startingOffsets": "latest",
        "failOnDataLoss": "false"
    }
        
    kafka_df = (spark.readStream
                .format("kafka")
                .options(**kafka_options)
                .load())
    
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), view_event_schema).alias("data")
    ).select("data.*")
    
    parsed_df = parsed_df.filter(col("collection").isin(args.allowed_collections))
    
    dim_time = process_dim_time(parsed_df)
    dim_browser = process_dim_browser(parsed_df)
    dim_os = process_dim_os(parsed_df)
    dim_location = process_dim_location(parsed_df)
    dim_product = process_dim_product(parsed_df)
    dim_referrer_url = process_dim_referrer_url(parsed_df)
    dim_store = process_dim_store(parsed_df)
    fact_view_event = process_fact_view_event(parsed_df)
    
    dim_time_query = create_query(dim_time, "dim_time", args)
    dim_browser_query = create_query(dim_browser, "dim_browser", args)
    dim_os_query = create_query(dim_os, "dim_os", args)
    dim_location_query = create_query(dim_location, "dim_location", args)
    dim_product_query = create_query(dim_product, "dim_product", args)
    dim_referrer_url_query = create_query(dim_referrer_url, "dim_referrer_url", args)
    dim_store_query = create_query(dim_store, "dim_store", args)
    fact_view_event_query = create_query(fact_view_event, "fact_view_event", args)

    logger.info("Starting streaming queries...")
    
    try:
        dim_time_query.awaitTermination()
        dim_browser_query.awaitTermination()
        dim_os_query.awaitTermination()
        dim_location_query.awaitTermination()
        dim_product_query.awaitTermination()
        dim_referrer_url_query.awaitTermination()
        dim_store_query.awaitTermination()
        fact_view_event_query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming queries...")
        dim_time_query.stop()
        dim_browser_query.stop()
        dim_os_query.stop()
        dim_location_query.stop()
        dim_product_query.stop()
        dim_referrer_url_query.stop()
        dim_store_query.stop()
        fact_view_event_query.stop()
        spark.stop()
        
        
def main():
    logger.info("Starting streaming job")
    args = parse_arguments()
    spark = create_spark_session(app_name="KafkaToPostgres")
    spark.sparkContext.setLogLevel("WARN")
    try:
        process(spark, args)
    finally:
        spark.stop()
        logger.info("Spark session stopped")
if __name__ == "__main__":
    main()
    

