from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import logging
import requests

class SparkHealthCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 host: str,
                 port: int,
                 required_workers_number: int,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.required_workers_number = required_workers_number
        self.logger = logging.getLogger(__name__)

    def _send_slack_alert(self, alert_message: str, context):
        try:
            SlackWebhookOperator(
                task_id=f'slack_notification_{context["task"].task_id}',
                slack_webhook_conn_id='slack_default',
                message=alert_message,
                username='Airflow Pipeline Alert Bot'
            ).execute(context)
        except Exception as e:
            self.logger.warning(f"Failed to send Slack alert: {e}")

    def execute(self, context):
        self.logger.info("Starting Spark health check...")

        url = f"http://{self.host}:{self.port}/json"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            payload = response.json()
        except Exception as e:
            raise AirflowException(f"Failed to connect to Spark Master at {url}: {e}")

        if payload.get("status") != "ALIVE":
            message = f"Spark Master is NOT ALIVE! Status: {payload.get('status')}"
            self._send_slack_alert(message, context)
            raise AirflowException(message)

        workers = payload.get("workers", [])
        total_worker = len(workers)
        alive_worker = sum(1 for w in workers if w.get("state") == "ALIVE")
        dead_workers = [w.get("host") for w in workers if w.get("state") != "ALIVE"]

        if alive_worker < self.required_workers_number:
            msg = (
                f"Not enough Spark workers ALIVE!\n"
                f"Required: {self.required_workers_number}, Alive: {alive_worker}\n"
                f"Dead Workers: {', '.join(dead_workers)}"
            )
            self._send_slack_alert(msg, context)
            raise AirflowException(msg)

        if alive_worker < total_worker:
            msg = (
                f"Some Spark workers are DEAD!\n"
                f"Alive: {alive_worker}/{total_worker}\n"
                f"Dead Workers: {', '.join(dead_workers)}"
            )
            self._send_slack_alert(msg, context)

        total_cores = payload.get("cores", 0)
        cores_used = payload.get("coresused", 0)
        total_mem = payload.get("memory", 0)
        mem_used = payload.get("memoryused", 0)

        core_usage_pct = (cores_used / total_cores) if total_cores else 0
        mem_usage_pct = (mem_used / total_mem) if total_mem else 0

        if core_usage_pct > 0.9:
            msg = f"High Spark CPU usage: {cores_used}/{total_cores} cores used ({core_usage_pct:.0%})"
            self._send_slack_alert(msg, context)

        if mem_usage_pct > 0.9:
            msg = f"High Spark memory usage: {mem_used}/{total_mem} MB used ({mem_usage_pct:.0%})"
            self._send_slack_alert(msg, context)

        self.logger.info(f"Spark health check passed. Workers: {alive_worker}/{total_worker}, CPU: {core_usage_pct:.0%}, MEM: {mem_usage_pct:.0%}")
