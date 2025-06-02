# from pendulum import datetime, duration
# from airflow import DAG
# from sensors.health_check_sensors import (
#     KafkaHealthCheckSensor,
#     PostgreSQLHealthCheckSensor,
#     SparkHealthCheckSensor
# )
# from notifiers.slack_notifier import send_slack_alert


# default_args = {
#     'owner': 'Nguyen Hai Quoc',
#     'retries': 3,
#     'retry_delay': duration(minutes=5),
#     'start_date': datetime(2025, 5, 22),
#     'on_failure_callback': send_slack_alert
# }
# with DAG(
#     dag_id="system_health_check",
#     default_args=default_args,
#     start_date=datetime(2025, 5, 22),
#     schedule_interval='@hourly',
#     catchup=False
# ) as dag:
    
#     health_check_kafka_task = KafkaHealthCheckSensor(
#         task_id="health_check_kafka",
#         bootstrap_servers="kafka-0:9092,kafka-1:9092,kafka-2:9092",
#         security_protocol="SASL_PLAINTEXT",
#         sasl_mechanism="PLAIN",
#         sasl_plain_username="kafka",
#         sasl_plain_password="admin"
#     )
    
#     health_check_spark_task = SparkHealthCheckSensor(
#         task_id="health_check_spark",
#         master_url="spark://spark-master:7077"
#     )
    
#     health_check_postgres_task = PostgreSQLHealthCheckSensor(
#         task_id="health_check_postgresql",
#         host="postgres-data",
#         port=5432,
#         user="postgres",
#         password="postgres"
#     )
    
#     health_check_kafka_task >> health_check_spark_task >> health_check_postgres_task
