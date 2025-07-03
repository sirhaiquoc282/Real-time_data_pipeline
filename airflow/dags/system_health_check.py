from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from operators.kafka_health_check_operator import KafkaHealthCheckOperator
from operators.postgresql_health_check_operator import PostgreSQLHealthCheckOperator
from operators.spark_health_check_operator import SparkHealthCheckOperator
import logging
from pendulum import datetime
from datetime import timedelta

logger = logging.getLogger(__name__)

def _send_slack_message(context, text_message: str):
    slack_notification = SlackWebhookOperator(
        task_id=f'slack_notification_{context["task"].task_id}',
        slack_webhook_conn_id='slack_default',
        message=text_message,
        username='Airflow Pipeline Alert Bot',
        trigger_rule='all_done'
    )
    try:
        slack_notification.execute(context=context)
    except AirflowException as e:
        logger.error(f"Failed to send Slack message: {e}")

def on_task_failure_callback(context):
    task_instance = context.get('task_instance')
    dag_run = context.get('dag_run')
    log_url = task_instance.log_url
    message = f":x: Task *{task_instance.task_id}* in DAG *{dag_run.dag_id}* failed.\nLog: {log_url}"
    _send_slack_message(context, text_message=message)

def on_dag_failure_callback(context):
    dag_run = context.get('dag_run')
    message = f":x: DAG *{dag_run.dag_id}* failed!"
    _send_slack_message(context, text_message=message)

default_args = {
    'owner': 'Nguyen Hai Quoc',
    'start_date': datetime(2025, 5, 22),
    'on_failure_callback': on_task_failure_callback
}

with DAG(
    dag_id="data_pipeline_end_to_end_check",
    default_args=default_args,
    description="Send alert via Slack on failure",
    schedule_interval=None,
    catchup=False,
    on_failure_callback=on_dag_failure_callback,
    tags=["healthcheck", "slack"],
) as dag:

    start = DummyOperator(task_id="start")

    kafka_check = KafkaHealthCheckOperator(
        task_id="check_kafka",
        bootstrap_servers="kafka-0:9092,kafka-1:9092,kafka-2:9092",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="kafka",
        sasl_plain_password="admin",
        required_brokers_number=3,
        topic="product_views",
        partitions=3,
        replicas_factor=3
    )

    postgres_check = PostgreSQLHealthCheckOperator(
        task_id="check_postgres",
        host="postgres-data",
        port=5432,
        user="postgres",
        password="postgres",
        dbname="glamira",
        tables=["fact_view_event","dim_time","dim_product","dim_location","dim_store","dim_referrer_url","dim_browser","dim_os"],
        schema="public"
    )

    spark_check = SparkHealthCheckOperator(
        task_id="check_spark",
        host="spark-master",
        port=8080,
        required_workers_number=2
    )
    

    end = DummyOperator(task_id="end", trigger_rule="all_done")

    start >> [kafka_check, postgres_check, spark_check] >> end
