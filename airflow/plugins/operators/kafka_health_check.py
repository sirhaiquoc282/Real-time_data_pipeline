from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import (
    NoBrokersAvailable,
    UnknownTopicOrPartitionError,
    GroupAuthorizationFailedError,
    KafkaError,
    UnsupportedVersionError
)
from typing import Optional, Dict, Any

class KafkaHealthCheckException(Exception):
    pass

class KafkaHealthCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        bootstrap_servers: str,
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: Optional[str] = None,
        sasl_plain_username: Optional[str] = None,
        sasl_plain_password: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password

        self._validate_security_config()

    def _validate_security_config(self):
        if "SASL" in self.security_protocol and not all([
            self.sasl_mechanism,
            self.sasl_plain_username,
            self.sasl_plain_password
        ]):
            raise ValueError(
                "SASL configuration requires mechanism, username and password"
            )

    def _build_kafka_config(self) -> Dict[str, Any]:
        config = {
            "bootstrap_servers": self.bootstrap_servers.split(','),
            "security_protocol": self.security_protocol,
        }

        if self.sasl_mechanism:
            config.update({
                "sasl_mechanism": self.sasl_mechanism,
                "sasl_plain_username": self.sasl_plain_username,
                "sasl_plain_password": self.sasl_plain_password,
            })

        return config

    def _check_broker_connection(self):
        admin = None
        try:
            admin = KafkaAdminClient(**self._build_kafka_config())
            cluster_metadata = admin.describe_cluster()
            
            if not (cluster_id := cluster_metadata.get('cluster_id')):
                raise KafkaHealthCheckException("Missing cluster ID in broker response")
                
            controller_id = cluster_metadata.get('controller_id')
            nodes = cluster_metadata.get('nodes', [])
            
            self.log.info(
                f"Connected to Kafka cluster: ID={cluster_id}, "
                f"Controller={controller_id}, Nodes={len(nodes)}"
            )
            
            if len(nodes) < 1:
                self.log.warning("Cluster has no available nodes")
                
        except NoBrokersAvailable as e:
            raise KafkaHealthCheckException(
                f"Connection failed to brokers: {self.bootstrap_servers}"
            ) from e
        except UnsupportedVersionError as e:
            raise KafkaHealthCheckException(f"Protocol error: {e}") from e
        except KafkaError as e:
            raise KafkaHealthCheckException(f"Kafka API error: {e}") from e
        finally:
            if admin:
                admin.close()

    def _check_topic_exists(self):
        if not self.topic:
            return

        admin = None
        try:
            admin = KafkaAdminClient(**self._build_kafka_config())
            if self.topic not in admin.list_topics():
                raise KafkaHealthCheckException(f"Topic '{self.topic}' not found")
            self.log.info(f"Topic verified: {self.topic}")
        except UnknownTopicOrPartitionError as e:
            raise KafkaHealthCheckException(f"Topic error: {e}") from e
        finally:
            if admin:
                admin.close()

    def _check_consumer_group(self):
        if not self.group_id:
            return

        config = self._build_kafka_config()
        config.update({
            "group_id": self.group_id,
            "enable_auto_commit": False,
            "auto_offset_reset": "earliest",
            "consumer_timeout_ms": 5000
        })

        consumer = None
        try:
            consumer = KafkaConsumer(**config)
            consumer.poll(timeout_ms=1000)
            if consumer.assignment():
                consumer.commit()
                self.log.info(f"Consumer group active: {self.group_id}")
            else:
                self.log.warning("No partitions assigned")
        except GroupAuthorizationFailedError as e:
            raise KafkaHealthCheckException(f"Group auth failed: {e}") from e
        finally:
            if consumer:
                consumer.close()

    def execute(self, context):
        self.log.info("Starting Kafka health checks...")
        checks = [
            self._check_broker_connection,
            self._check_topic_exists,
            self._check_consumer_group
        ]
        for check in checks:
            try:
                check()
            except KafkaHealthCheckException as e:
                self.log.error(f"Health check failed: {e}")
                raise
            except Exception as e:
                raise KafkaHealthCheckException(f"Unexpected error: {e}") from e
        self.log.info("All health checks passed!")