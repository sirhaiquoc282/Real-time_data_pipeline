from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import psycopg2
from kafka import KafkaConsumer
from airflow.exceptions import AirflowException
from pyspark.sql import SparkSession
import logging



logger = logging.getLogger(__name__)

class KafkaHealthCheckSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(
        self, 
        bootstrap_servers: str, 
        security_protocol: str = "PLAINTEXT", 
        sasl_mechanism: str = None, 
        sasl_plain_username: str = None, 
        sasl_plain_password: str = None, 
        topic: str = None, 
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password
        
    def poke(self, context):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers.split(','),
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_plain_username,
                sasl_plain_password=self.sasl_plain_password
            )
            consumer.topics()
            consumer.close()
            logger.info("Kafka connection successful")
            return True
        except Exception as e:
            logger.error(f"Kafka health check failed: {str(e)}")
            raise AirflowException(f"Kafka health check failed: {str(e)}")
        
class PostgreSQLHealthCheckSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(
        self, 
        host: str, 
        port: int,
        user: str,
        password: str
    ):
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
    def poke(self, context):
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            conn.close()
            logger.info("PostgreSQL connection successful")
            return True
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL health check failed: {str(e)}")
            raise AirflowException(f"PostgreSQL health check failed: {str(e)}")

class SparkHealthCheckSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, master_url: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.master_url = master_url
    def poke(self, context):
        try:
            spark = SparkSession.builder \
                .master(self.master_url) \
                .appName("HealthCheck") \
                .getOrCreate()
            spark.range(1).count()
            spark.stop()
            logger.info("Spark connection successful")
            return True
        except Exception as e:
            logger.error(f"Spark health check failed: {str(e)}")
            raise AirflowException(f"Spark health check failed: {str(e)}")