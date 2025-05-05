from pyspark.sql import SparkSession, DataFrame
from configparser import SafeConfigParser
import logging


def load_config(config_file: str) -> SafeConfigParser:
    try:
        parser = SafeConfigParser()
        parser.read(config_file)
        return parser
    except Exception as e:
        logging.error(f"Error loading config file: {e}")
        raise


def get_spark_session(master: str, app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).master(master).getOrCreate()


def read_kafka_stream(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    kafka_topic: str,
    kafka_starting_offsets: str,
    kafka_max_offsets_per_trigger: int,
    kafka_security_protocol: str,
    kafka_sasl_mechanism,
    kafka_sasl_jaas_config: str,
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", kafka_starting_offsets)
        .option("maxOffsetsPerTrigger", kafka_max_offsets_per_trigger)
        .option("kafka.security.protocol", kafka_security_protocol)
        .option("kafka.sasl.mechanism", kafka_sasl_mechanism)
        .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config)
        .load()
        .selectExpr("CAST(value AS STRING)")
    )


def write_to_mongodb(
    df: DataFrame, mongo_uri: str, mongo_database: str, mongo_collection: str
) -> DataFrame:
    return (
        df.writeStream.format("mongodb")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/kafka_to_mongo_checkpoint")
        .option("forceDeleteTempCheckpointLocation", "true")
        .option("spark.mongodb.connection.uri", mongo_uri)
        .option("spark.mongodb.database", mongo_database)
        .option("spark.mongodb.collection", mongo_collection)
        .start()
    )


def main():
    config = load_config("airflow/config/config.ini")
    kafka_bootstrap_servers = config.get("KAFKA", "KAFKA_BOOTSTRAP_SERVERS")
    kafka_security_protocol = config.get("KAFKA", "KAFKA_SECURITY_PROTOCOL")
    kafka_sasl_mechanism = config.get("KAFKA", "KAFKA_SASL_MECHANISM")
    kafka_sasl_jaas_config = config.get("KAFKA", "KAFKA_SASL_JAAS_CONFIG")
    kafka_max_offsets_per_trigger = config.getint(
        "KAFKA", "KAFKA_MAX_OFFSETS_PER_TRIGGER"
    )
    kafka_starting_offsets = config.get("KAFKA", "KAFKA_STARTING_OFFSETS")
    kafka_topic = config.get("KAFKA", "KAFKA_TOPIC")

    spark_master = config.get("SPARK", "SPARK_MASTER")

    mongo_uri = config.get("MONGO", "MONGO_URI")
    mongo_database = config.get("MONGO", "MONGO_DB")
    mongo_collection = config.get("MONGO", "MONGO_COLLECTION")

    spark = get_spark_session(master=spark_master, app_name="StreamingPipeline")
    kafka_df = read_kafka_stream(
        spark=spark,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic,
        kafka_starting_offsets=kafka_starting_offsets,
        kafka_max_offsets_per_trigger=kafka_max_offsets_per_trigger,
        kafka_security_protocol=kafka_security_protocol,
        kafka_sasl_mechanism=kafka_sasl_mechanism,
        kafka_sasl_jaas_config=kafka_sasl_jaas_config,
    )

    query = write_to_mongodb(
        kafka_df,
        mongo_uri=mongo_uri,
        mongo_database=mongo_database,
        mongo_collection=mongo_collection,
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
