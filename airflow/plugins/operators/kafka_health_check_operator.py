from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from airflow.exceptions import AirflowException
import logging
import uuid
import json

class KafkaHealthCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 bootstrap_servers: str,
                 security_protocol: str,
                 sasl_mechanism: str,
                 sasl_plain_username: str,
                 sasl_plain_password: str,
                 required_brokers_number: int,
                 topic: str,
                 partitions: int,
                 replicas_factor: int,
                 canary_message: str = "kafka_health_check",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password
        self.required_brokers_number = required_brokers_number
        self.topic = topic
        self.partitions = partitions
        self.replicas_factor = replicas_factor
        self.canary_message = canary_message
        self.logger = logging.getLogger(__name__)

    def _check_connection(self):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_plain_username,
                sasl_plain_password=self.sasl_plain_password
            )
            admin.list_topics()
            self.logger.info("Connected to Kafka cluster.")
        except Exception as e:
            raise AirflowException(f"Kafka connection failed: {e}")
        finally:
            admin.close()

    def _check_kafka_cluster(self):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_plain_username,
                sasl_plain_password=self.sasl_plain_password
            )

            cluster_info = admin.describe_cluster()
            brokers = cluster_info.get('brokers', [])
            if len(brokers) < self.required_brokers_number:
                raise AirflowException(f"Brokers: {len(brokers)} < required {self.required_brokers_number}")

            topics = admin.list_topics()
            if self.topic not in topics:
                raise AirflowException(f"Topic '{self.topic}' not found.")

            topic_metadata = admin.describe_topics([self.topic])[0]
            num_partitions = len(topic_metadata['partitions'])
            replicas = len(topic_metadata['partitions'][0]['replicas'])


            if num_partitions < self.partitions:
                raise AirflowException(f"Topic '{self.topic}' has {num_partitions} partitions, requires {self.partitions}")

            if replicas < self.replicas_factor:
                raise AirflowException(f"Topic '{self.topic}' has {replicas} replicas, requires {self.replicas_factor}")

            self.logger.info(f"Cluster OK: {len(brokers)} brokers, topic '{self.topic}' has {num_partitions} partitions and {replicas} replicas.")

        except Exception as e:
            raise AirflowException(f"Kafka cluster check failed: {e}")
        finally:
            admin.close()

    def _send_canary_message(self,context):
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_plain_username,
                sasl_plain_password=self.sasl_plain_password,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )

            message_id = str(uuid.uuid4())
            payload = {
                "message_id": message_id,
                "message": self.canary_message,
                "timestamp": str(context['execution_date']),
                "type": "canary"
            }

            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_plain_username,
                sasl_plain_password=self.sasl_plain_password,
                auto_offset_reset='latest',
                consumer_timeout_ms=10000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            producer.send(self.topic, value=payload)
            producer.flush()
            self.logger.info(f"Canary message sent to topic '{self.topic}'.")

            for msg in consumer:
                if msg.value.get("message_id") == message_id:
                    self.logger.info(f"Canary message received from topic '{self.topic}'.")
                    context['ti'].xcom_push(key="kafka_health_message_id", value=message_id)
                    return
                else:
                    raise AirflowException("Canary message not received within timeout.")

        except Exception as e:
            raise AirflowException(f"Kafka canary message check failed: {e}")
        finally:
            if producer:
                producer.close()
            if consumer:
                consumer.close()

    def execute(self, context):
        self.logger.info("Starting Kafka health check...")
        self._check_connection()
        self._check_kafka_cluster()
        self._send_canary_message(context)
        self.logger.info("Kafka health check passed.")
