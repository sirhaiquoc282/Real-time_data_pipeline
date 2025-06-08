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
    """
    Write a batch of data to PostgreSQL database with conflict resolution.
    
    This function connects to PostgreSQL and performs an upsert operation using
    INSERT ... ON CONFLICT DO NOTHING to handle duplicate records based on
    the primary key of each table.
    
    Args:
        batch (pyspark.sql.DataFrame): The DataFrame containing data to be written
        table_name (str): The name of the target PostgreSQL table
        configs (dict): Configuration dictionary containing PostgreSQL connection parameters
                       with structure: configs["postgresql"]["host|port|user|password|dbname"]
    
    Returns:
        None
        
    Raises:
        psycopg2.Error: For PostgreSQL-specific errors during connection or execution
        Exception: For any other unexpected errors during the process
        
    Note:
        - For 'fact_view_event' table, uses 'event_id' as conflict column
        - For dimension tables, constructs conflict column as '{table_suffix}_id'
        - Uses batch processing with page_size=100 for optimal performance
        - Automatically handles connection cleanup and error rollback
    """
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
    """
    Process a single batch of streaming data and write to PostgreSQL.
    
    This function is called for each micro-batch in the Spark Structured Streaming
    process. It processes the raw streaming data into dimension and fact tables
    following a star schema design pattern, then writes all processed data to
    PostgreSQL database.
    
    Args:
        batch (pyspark.sql.DataFrame): The micro-batch DataFrame containing streaming data
        epoch_id (int): Unique identifier for the current batch/epoch
        spark (SparkSession): The active Spark session
        configs (dict): Configuration dictionary containing database and processing settings
    
    Returns:
        None
        
    Raises:
        Exception: Any error during data processing or database writing operations
        
    Processing Flow:
        1. Extract and process dimension tables (time, browser, OS, location, product, referrer_url, store)
        2. Process fact table (view_event) with foreign key references
        3. Write all processed data to PostgreSQL in sequence
        4. Log success or failure of the batch processing
        
    Note:
        - Processes data following star schema with dimensions loaded before facts
        - Each dimension and fact table is written independently with error handling
        - Logs processing progress and any errors encountered
    """
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
    """
    Create and configure a Spark Structured Streaming query.
    
    This function sets up the streaming query with specific configuration for
    processing Kafka data in micro-batches and writing to PostgreSQL. It configures
    checkpointing, output mode, and trigger intervals for reliable stream processing.
    
    Args:
        parsed_df (pyspark.sql.DataFrame): The parsed DataFrame from Kafka stream
        spark (SparkSession): The active Spark session
        configs (dict): Configuration dictionary (passed to batch processing function)
    
    Returns:
        pyspark.sql.streaming.StreamingQuery: The configured and started streaming query
        
    Configuration Details:
        - foreachBatch: Uses custom batch processing function for each micro-batch
        - checkpointLocation: Enables fault tolerance with checkpoint storage at /opt/bitnami/spark/checkpoints/
        - outputMode: Set to "append" for incremental processing
        - trigger: Processes data every 30 seconds (processingTime="30 seconds")
        
    Note:
        - The query starts immediately upon creation
        - Checkpoint location should be on a reliable, distributed file system in production
        - Processing time interval can be adjusted based on data volume and latency requirements
    """
    return (parsed_df.writeStream
        .foreachBatch(lambda batch, epoch_id: process_batch(batch, epoch_id, spark, configs))
        .option("checkpointLocation", "/opt/bitnami/spark/checkpoints/")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start())


            
def process(spark: SparkSession, configs):
    """
    Main processing function that sets up Kafka streaming and data processing pipeline.
    
    This function establishes the complete data pipeline from Kafka consumption to
    PostgreSQL storage. It configures Kafka connection with SASL authentication,
    parses incoming JSON messages, filters data based on allowed collections,
    and starts the streaming query processing.
    
    Args:
        spark (SparkSession): The active Spark session for distributed processing
        configs (dict): Complete configuration dictionary containing:
                       - kafka: bootstrap_servers, security_protocol, sasl_mechanism, 
                               sasl_plain_username, sasl_plain_password, topic, allowed_collections
                       - postgresql: connection parameters
    
    Returns:
        None
        
    Pipeline Flow:
        1. Configure Kafka connection options with SASL/PLAIN authentication
        2. Create streaming DataFrame from Kafka topic
        3. Parse JSON message values using predefined schema
        4. Filter data by allowed collections
        5. Start streaming query with batch processing
        6. Wait for termination or handle keyboard interrupt
        
    Raises:
        KeyboardInterrupt: Gracefully handles manual termination
        Exception: Any error during streaming setup or processing
        
    Note:
        - Uses "latest" starting offset to process only new messages
        - Disables failOnDataLoss for better fault tolerance
        - Includes graceful shutdown handling for production deployment
    """
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
    """
    Entry point for the Kafka to PostgreSQL streaming application.
    
    This function initializes the streaming job by loading configuration,
    creating the Spark session, and starting the data processing pipeline.
    It handles the complete lifecycle of the streaming application including
    setup, execution, and cleanup.
    
    Returns:
        None
        
    Execution Flow:
        1. Load application configuration from config files
        2. Create Spark session with appropriate settings for Kafka processing
        3. Set Spark log level to WARN to reduce verbose logging
        4. Start the main processing pipeline
        5. Ensure proper cleanup of Spark resources regardless of exit condition
        
    Note:
        - Uses try-finally block to guarantee Spark session cleanup
        - Logs application lifecycle events for monitoring
        - Should be called when running the script directly
        - Suitable for deployment in production streaming environments
    """
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