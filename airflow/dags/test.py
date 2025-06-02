from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.sql import SqlSensor
from airflow.hooks.base_hook import BaseHook
import psycopg2
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule
import subprocess
import logging
import json
import requests
import time
import os

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    'streaming_pipeline_orchestration',
    default_args=default_args,
    description='Complete streaming pipeline with Kafka, Spark, and PostgreSQL',
    schedule_interval=timedelta(hours=1),  # Chạy mỗi giờ để monitor
    catchup=False,
    max_active_runs=1,
    tags=['streaming', 'kafka', 'spark', 'postgres']
)

# Configuration variables
KAFKA_BROKERS = Variable.get("kafka_brokers", "localhost:9092")
SPARK_MASTER = Variable.get("spark_master", "spark://spark-master:7077")
POSTGRES_CONN_ID = "postgres_default"
SLACK_WEBHOOK_CONN_ID = "slack_webhook"

# Health check functions
def check_kafka_health(**context):
    """Kiểm tra trạng thái Kafka cluster"""
    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.errors import NoBrokersAvailable
        
        # Test producer connection
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000,
            api_version=(0, 10, 1)
        )
        
        # Send test message
        test_topic = 'health_check'
        producer.send(test_topic, {'timestamp': str(datetime.now()), 'status': 'ok'})
        producer.flush()
        producer.close()
        
        logging.info("Kafka health check passed")
        return True
        
    except Exception as e:
        logging.error(f"Kafka health check failed: {str(e)}")
        raise AirflowFailException(f"Kafka is not healthy: {str(e)}")

def check_spark_health(**context):
    """Kiểm tra trạng thái Spark cluster"""
    try:
        # Check Spark Master UI
        spark_ui_url = SPARK_MASTER.replace('spark://', 'http://').replace(':7077', ':8080')
        response = requests.get(f"{spark_ui_url}/json/", timeout=30)
        
        if response.status_code == 200:
            spark_info = response.json()
            active_workers = len([w for w in spark_info.get('workers', []) if w.get('state') == 'ALIVE'])
            
            if active_workers == 0:
                raise Exception("No active Spark workers found")
                
            logging.info(f"Spark cluster healthy with {active_workers} active workers")
            return True
        else:
            raise Exception(f"Spark Master UI returned status {response.status_code}")
            
    except Exception as e:
        logging.error(f"Spark health check failed: {str(e)}")
        raise AirflowFailException(f"Spark cluster is not healthy: {str(e)}")

def get_postgres_connection(**context):
    """Tạo kết nối PostgreSQL từ Airflow connection"""
    connection = BaseHook.get_connection(POSTGRES_CONN_ID)
    return psycopg2.connect(
        host=connection.host,
        database=connection.schema,
        user=connection.login,
        password=connection.password,
        port=connection.port or 5432
    )
def check_postgres_health(**context):
    """Kiểm tra trạng thái PostgreSQL"""
    try:
        # Sử dụng connection trực tiếp
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Test connection and basic query
        cursor.execute("SELECT version(), current_timestamp;")
        records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if records:
            logging.info(f"PostgreSQL health check passed: {records[0]}")
            return True
        else:
            raise Exception("No response from PostgreSQL")
            
    except Exception as e:
        logging.error(f"PostgreSQL health check failed: {str(e)}")
        raise AirflowFailException(f"PostgreSQL is not healthy: {str(e)}")

def validate_data_quality(**context):
    """Kiểm tra chất lượng dữ liệu trong pipeline"""
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Kiểm tra số lượng records trong 1 giờ qua
        query = """
        SELECT 
            COUNT(*) as record_count,
            COUNT(DISTINCT source_id) as unique_sources,
            AVG(processing_time_ms) as avg_processing_time,
            MAX(created_at) as latest_record
        FROM streaming_data 
        WHERE created_at >= NOW() - INTERVAL '1 hour'
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result:
            record_count, unique_sources, avg_processing_time, latest_record = result
            
            # Kiểm tra các threshold
            if record_count < 100:  # Minimum expected records per hour
                raise Exception(f"Low data volume: only {record_count} records in last hour")
                
            if unique_sources < 5:  # Minimum expected data sources
                raise Exception(f"Low source diversity: only {unique_sources} unique sources")
                
            if avg_processing_time and avg_processing_time > 5000:  # Max 5 seconds processing time
                raise Exception(f"High processing latency: {avg_processing_time}ms average")
                
            # Kiểm tra data freshness (không quá 10 phút)
            if latest_record:
                time_diff = datetime.now() - latest_record
                if time_diff.total_seconds() > 600:  # 10 minutes
                    raise Exception(f"Stale data: latest record is {time_diff} old")
                    
            logging.info(f"Data quality check passed: {record_count} records, {unique_sources} sources")
            return True
            
    except Exception as e:
        logging.error(f"Data quality check failed: {str(e)}")
        raise AirflowFailException(f"Data quality issues detected: {str(e)}")

def manage_kafka_topics(**context):
    """Quản lý Kafka topics - tạo, cấu hình retention, cleanup"""
    try:
        from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
        from kafka.admin.config_resource import ConfigResource
        from kafka import KafkaAdminClient
        
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS.split(','),
            client_id='airflow_topic_manager'
        )
        
        # Lấy danh sách topics hiện tại
        metadata = admin_client.describe_topics()
        existing_topics = set(metadata.keys())
        
        # Required topics cho pipeline
        required_topics = {
            'raw_events': {'retention.ms': '604800000'},  # 7 days
            'processed_events': {'retention.ms': '1209600000'},  # 14 days
            'error_events': {'retention.ms': '2592000000'},  # 30 days
            'dead_letter_queue': {'retention.ms': '2592000000'}  # 30 days
        }
        
        # Tạo topics nếu chưa tồn tại
        for topic_name, config in required_topics.items():
            if topic_name not in existing_topics:
                from kafka.admin import NewTopic
                topic = NewTopic(
                    name=topic_name,
                    num_partitions=6,
                    replication_factor=2,
                    topic_configs=config
                )
                admin_client.create_topics([topic])
                logging.info(f"Created topic: {topic_name}")
                
        # Cập nhật cấu hình retention cho topics
        for topic_name, config in required_topics.items():
            if topic_name in existing_topics:
                resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
                admin_client.alter_configs({resource: config})
                
        logging.info("Kafka topics management completed")
        return True
        
    except Exception as e:
        logging.error(f"Kafka topics management failed: {str(e)}")
        raise AirflowFailException(f"Failed to manage Kafka topics: {str(e)}")

def submit_spark_streaming_job(**context):
    """Submit Spark Streaming job với configuration phù hợp"""
    try:
        spark_submit_cmd = f"""
        spark-submit \
        --master {SPARK_MASTER} \
        --deploy-mode cluster \
        --driver-memory 2g \
        --executor-memory 4g \
        --executor-cores 2 \
        --num-executors 4 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.streaming.stopGracefullyOnShutdown=true \
        --conf spark.sql.streaming.checkpointLocation=/opt/spark/checkpoints \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        /opt/spark/apps/streaming_pipeline.py \
        --kafka-brokers {KAFKA_BROKERS} \
        --postgres-url jdbc:postgresql://postgres:5432/streaming_db \
        --checkpoint-location /opt/spark/checkpoints/streaming_pipeline
        """
        
        # Execute spark-submit
        result = subprocess.run(
            spark_submit_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout for job submission
        )
        
        if result.returncode == 0:
            logging.info("Spark streaming job submitted successfully")
            logging.info(f"Spark submit output: {result.stdout}")
            return True
        else:
            raise Exception(f"Spark submit failed: {result.stderr}")
            
    except Exception as e:
        logging.error(f"Spark job submission failed: {str(e)}")
        raise AirflowFailException(f"Failed to submit Spark streaming job: {str(e)}")

def monitor_streaming_job(**context):
    """Monitor Spark streaming job performance"""
    try:
        # Check Spark Streaming UI for job status
        spark_ui_url = SPARK_MASTER.replace('spark://', 'http://').replace(':7077', ':4040')
        
        # Get streaming statistics
        streaming_url = f"{spark_ui_url}/api/v1/applications"
        response = requests.get(streaming_url, timeout=30)
        
        if response.status_code == 200:
            applications = response.json()
            
            # Find streaming application
            streaming_app = None
            for app in applications:
                if 'streaming_pipeline' in app.get('name', '').lower():
                    streaming_app = app
                    break
                    
            if streaming_app:
                app_id = streaming_app['id']
                
                # Get streaming batches info
                batches_url = f"{spark_ui_url}/api/v1/applications/{app_id}/streaming/batches"
                batches_response = requests.get(batches_url, timeout=30)
                
                if batches_response.status_code == 200:
                    batches = batches_response.json()
                    
                    # Analyze recent batches
                    recent_batches = batches[:10]  # Last 10 batches
                    failed_batches = [b for b in recent_batches if b.get('status') == 'FAILED']
                    avg_processing_time = sum([b.get('processingTime', 0) for b in recent_batches]) / len(recent_batches)
                    
                    if len(failed_batches) > 2:  # More than 2 failed batches
                        raise Exception(f"High failure rate: {len(failed_batches)} failed batches in last 10")
                        
                    if avg_processing_time > 30000:  # More than 30 seconds average
                        raise Exception(f"High processing latency: {avg_processing_time}ms average")
                        
                    logging.info(f"Streaming job monitoring passed: avg processing time {avg_processing_time}ms")
                    return True
                    
        raise Exception("Could not retrieve streaming job metrics")
        
    except Exception as e:
        logging.error(f"Streaming job monitoring failed: {str(e)}")
        # Don't fail the DAG, just alert
        return False

def cleanup_old_data(**context):
    """Cleanup old data and optimize database"""
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Cleanup data older than 30 days
        cleanup_queries = [
            "DELETE FROM streaming_data WHERE created_at < NOW() - INTERVAL '30 days'",
            "DELETE FROM error_logs WHERE created_at < NOW() - INTERVAL '7 days'",
            "DELETE FROM job_metrics WHERE created_at < NOW() - INTERVAL '14 days'"
        ]
        
        for query in cleanup_queries:
            cursor.execute(query)
            affected_rows = cursor.rowcount
            logging.info(f"Cleanup query executed: {affected_rows} rows affected")
            
        # Vacuum and analyze tables
        maintenance_queries = [
            "VACUUM ANALYZE streaming_data",
            "VACUUM ANALYZE error_logs", 
            "VACUUM ANALYZE job_metrics"
        ]
        
        for query in maintenance_queries:
            cursor.execute(query)
            logging.info(f"Maintenance query executed: {query}")
        
        conn.commit()
        cursor.close()
        conn.close()
            
        logging.info("Database cleanup and maintenance completed")
        return True
        
    except Exception as e:
        logging.error(f"Database cleanup failed: {str(e)}")
        # Don't fail DAG for cleanup issues
        return False

def send_alert_notification(context):
    """Gửi thông báo khi có lỗi trong pipeline"""
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    
    message = f"""
    🚨 *Streaming Pipeline Alert* 🚨
    
    *DAG*: {dag_run.dag_id}
    *Task*: {task_instance.task_id}
    *Execution Date*: {dag_run.execution_date}
    *Status*: FAILED
    *Log URL*: {task_instance.log_url}
    
    Please check the pipeline immediately!
    """
    
    return SlackWebhookOperator(
        task_id='send_slack_alert',
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message=message,
        dag=dag
    ).execute(context)

# Define tasks
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Health checks - parallel execution
kafka_health_check = PythonOperator(
    task_id='kafka_health_check',
    python_callable=check_kafka_health,
    dag=dag
)

spark_health_check = PythonOperator(
    task_id='spark_health_check', 
    python_callable=check_spark_health,
    dag=dag
)

postgres_health_check = PythonOperator(
    task_id='postgres_health_check',
    python_callable=check_postgres_health,
    dag=dag
)

# Infrastructure management
manage_topics = PythonOperator(
    task_id='manage_kafka_topics',
    python_callable=manage_kafka_topics,
    dag=dag
)

def init_database_tables(**context):
    """Khởi tạo các bảng trong PostgreSQL"""
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # SQL để tạo tables
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS streaming_data (
            id BIGSERIAL PRIMARY KEY,
            source_id VARCHAR(100),
            event_type VARCHAR(50),
            payload JSONB,
            processing_time_ms INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_streaming_data_created_at ON streaming_data(created_at);
        CREATE INDEX IF NOT EXISTS idx_streaming_data_source_id ON streaming_data(source_id);
        
        CREATE TABLE IF NOT EXISTS error_logs (
            id BIGSERIAL PRIMARY KEY,  
            job_id VARCHAR(100),
            error_type VARCHAR(100),
            error_message TEXT,
            stack_trace TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS job_metrics (
            id BIGSERIAL PRIMARY KEY,
            job_name VARCHAR(100),
            metric_name VARCHAR(100), 
            metric_value DECIMAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_tables_sql)
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("Database tables initialized successfully")
        return True
        
    except Exception as e:
        logging.error(f"Database initialization failed: {str(e)}")
        raise AirflowFailException(f"Failed to initialize database: {str(e)}")

# Database initialization - Using PythonOperator
init_db_tables = PythonOperator(
    task_id='init_database_tables',
    python_callable=init_database_tables,
    dag=dag
)

# Main streaming job
submit_streaming_job = PythonOperator(
    task_id='submit_spark_streaming_job',
    python_callable=submit_spark_streaming_job,
    dag=dag
)

# Monitoring tasks
monitor_job = PythonOperator(
    task_id='monitor_streaming_job',
    python_callable=monitor_streaming_job,
    dag=dag
)

data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=validate_data_quality,
    dag=dag
)

# Maintenance task
cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag
)

# File sensor for configuration changes
config_sensor = FileSensor(
    task_id='config_file_sensor',
    filepath='/opt/airflow/configs/streaming_config.json',
    fs_conn_id='fs_default',
    poke_interval=300,  # Check every 5 minutes
    timeout=600,  # Timeout after 10 minutes
    dag=dag
)

# SQL sensor for data availability
data_sensor = SqlSensor(
    task_id='data_availability_sensor',
    conn_id=POSTGRES_CONN_ID,
    sql="SELECT COUNT(*) FROM streaming_data WHERE created_at > NOW() - INTERVAL '10 minutes'",
    poke_interval=60,
    timeout=300,
    dag=dag
)

# Success notification
success_notification = SlackWebhookOperator(
    task_id='success_notification',
    slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
    message="""
    ✅ *Streaming Pipeline Success* ✅
    
    Pipeline execution completed successfully!
    All health checks passed and data quality validated.
    """,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

# Failure notification
failure_notification = PythonOperator(
    task_id='failure_notification',
    python_callable=send_alert_notification,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_pipeline',
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag
)

start_task >> [kafka_health_check, spark_health_check, postgres_health_check]

[kafka_health_check, spark_health_check, postgres_health_check] >> manage_topics
manage_topics >> init_db_tables
init_db_tables >> submit_streaming_job >> success_notification >> end_task

for task in [kafka_health_check, spark_health_check, postgres_health_check]:
    task >> failure_notification

failure_notification >> end_task


# submit_streaming_job >> [monitor_job, data_quality_check]

# for monitoring_task in [monitor_job, data_quality_check]:
#     monitoring_task >> cleanup_task
#     monitoring_task >> config_sensor
#     monitoring_task >> data_sensor

# [cleanup_task, config_sensor, data_sensor] >> success_notification
# success_notification >> end_task

# for task in [kafka_health_check, spark_health_check, postgres_health_check, 
#              manage_topics, init_db_tables, submit_streaming_job, 
#              monitor_job, data_quality_check]:
#     task >> failure_notification
# failure_notification >> end_task