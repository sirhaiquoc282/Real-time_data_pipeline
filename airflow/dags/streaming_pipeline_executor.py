from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from operators.kafka_health_check import KafkaHealthCheckOperator
from operators.mongodb_health_check import MongoDBHealthCheckOperator
from operators.spark_health_check import SparkHealthCheckOperator
from operators.postgresql_health_check import PostgreSQLHealthCheckOperator
from pendulum import datetime, duration

default_args = {
    'owner': 'Nguyen Hai Quoc',
    'retries': 3,
    'retry_delay': duration(minutes=5),
    'start_date': datetime(2025,5,22)
}

with DAG(
    dag_id = "execute_streaming_data_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    
    health_check_kafka_task = KafkaHealthCheckOperator(
        task_id="health_check_kafka",
        bootstrap_servers="kafka-0:9092,kafka-1:9092,kafka-2:9092",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="kafka",
        sasl_plain_password="admin",
        dag=dag
    )
    
    health_check_mongodb_task = MongoDBHealthCheckOperator(
        task_id="health_check_mongodb",
        uri="mongodb://mongo:mongo@mongodb:27017/glamira",
        dag=dag
    )
    
    health_check_spark_task = SparkHealthCheckOperator(
        task_id="health_check_spark",
        master_url = "spark://spark-master:7077",
        dag=dag
    )
    
    health_check_postgres_task = PostgreSQLHealthCheckOperator(
        task_id="health_check_postgresql",
        host="postgres-data",
        port=5432,
        user="postgres",
        password="postgres",
        dag=dag
    )
    


[health_check_kafka_task, health_check_mongodb_task, health_check_spark_task, health_check_postgres_task]

