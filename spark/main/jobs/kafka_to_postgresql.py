import logging
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql.functions import col, from_json
from main.utils.config_loader import load_config
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

def write_to_postgresql(batch, table_name, configs):
    import psycopg2
    from psycopg2 import extras, sql
    if table_name == "fact_view_event":
        conflict_column = "event_id"
    else:
        conflict_column = "_".join(table_name.split("_")[1:]) + "_id"
    
    try:
        rows = batch.collect()
        if not rows:
            logger.info(f"No data to write to {table_name}")
            return
            
        columns = batch.columns
    except Exception as e:
        logger.error(f"Failed to collect data: {str(e)}")
        return
    columns = batch.columns

    conn = None
    try:
        conn = psycopg2.connect(
            host=configs["postgresql"]["host"],
            port=configs["postgresql"]["port"],
            user=configs["postgresql"]["user"],
            password=configs["postgresql"]["password"],
            dbname=configs["postgresql"]["dbname"]
        )
        cursor = conn.cursor()
        
        insert_stmt = sql.SQL("""
            INSERT INTO {table} ({fields})
            VALUES %s
            ON CONFLICT ({conflict_column}) DO NOTHING
        """).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
            conflict_column=sql.Identifier(conflict_column)
        )
        
        data_tuples = [tuple(row) for row in rows]
        
        extras.execute_values(
            cursor,
            insert_stmt,
            data_tuples,
            template=None,
            page_size=100
        )
        
        conn.commit()
        logger.info(f"Successfully wrote {len(rows)} rows to {table_name}")
        
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL error: {str(e)}")
        if conn:
            conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def process_batch(batch: DataFrame, epoch_id: int, spark: SparkSession ,configs):
    try:
        logger.info(f"Processing batch {epoch_id}")
        dim_time = process_dim_time(batch)
        dim_browser = process_dim_browser(batch)
        dim_os = process_dim_os(batch)
        dim_location = process_dim_location(batch)
        dim_product = process_dim_product(batch)
        dim_referrer_url = process_dim_referrer_url(batch)
        dim_store = process_dim_store(batch)
        fact_view_event = process_fact_view_event(batch)

        write_to_postgresql(dim_time, "dim_time", configs)
        write_to_postgresql(dim_browser, "dim_browser",  configs)
        write_to_postgresql(dim_os, "dim_os", configs)
        write_to_postgresql(dim_product, "dim_product", configs)
        write_to_postgresql(dim_referrer_url, "dim_referrer_url", configs)
        write_to_postgresql(dim_store, "dim_store", configs)
        write_to_postgresql(dim_location,"dim_location", configs)
        write_to_postgresql(fact_view_event, "fact_view_event", configs)
        logger.info(f"Processed batch {epoch_id} successfully")

    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {e}")
                    

            
def create_query(parsed_df: DataFrame, spark: SparkSession ,configs):
    return (parsed_df.writeStream
        .foreachBatch(lambda batch, epoch_id: process_batch(batch, epoch_id, spark, configs))
        .option("checkpointLocation", "/opt/bitnami/spark/checkpoints/")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start())


            
def process(spark: SparkSession, configs):
    kafka_options = {
        "kafka.bootstrap.servers": configs["kafka"]["bootstrap_servers"],
        "kafka.security.protocol": configs["kafka"]["security_protocol"],
        "kafka.sasl.mechanism": configs["kafka"]["sasl_mechanism"],
        "kafka.sasl.jaas.config": 
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username=\"{configs['kafka']['sasl_plain_username']}\" "
            f"password=\"{configs['kafka']['sasl_plain_password']}\";",
        "subscribe": configs["kafka"]["topic"],
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
    
    parsed_df = parsed_df.filter(col("collection").isin(configs["kafka"]["allowed_collections"]))
    
    query = create_query(parsed_df, spark, configs)

    logger.info("Starting streaming queries...")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming queries...")
        query.stop()
        spark.stop()
        
        
def main():
    logger.info("Starting streaming job")
    configs = load_config()
    spark = create_spark_session(app_name="KafkaToPostgres")
    spark.sparkContext.setLogLevel("WARN")
    try:
        process(spark, configs)
    finally:
        spark.stop()
        logger.info("Spark session stopped")
if __name__ == "__main__":
    main()