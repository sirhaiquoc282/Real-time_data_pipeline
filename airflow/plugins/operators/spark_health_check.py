from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession
from pyspark import SparkConf
from typing import Optional
import requests

class SparkHealthCheckException(Exception):
    pass

class SparkHealthCheckOperator(BaseOperator):
    
    @apply_defaults
    def __init__(
        self,
        master_url: str,
        app_name: Optional[str] = "spark-health-check",
        min_workers: Optional[int] = 0,
        timeout: Optional[int] = 5,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.master_url = master_url
        self.app_name = app_name
        self.min_workers = min_workers
        self.timeout = timeout

    def _check_worker_availability(self):
        try:
            host = self.master_url.split("//")[-1].split(":")[0]
            api_url = f"http://{host}:8080/json/"
            
            response = requests.get(api_url, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            alive_workers = data.get("aliveworkers", 0)
            
            self.log.info(f"Detected {alive_workers} alive workers")
            
            if alive_workers < self.min_workers:
                raise SparkHealthCheckException(
                    f"Worker check failed: {alive_workers}/{self.min_workers} workers available"
                )
                
        except requests.exceptions.RequestException as e:
            raise SparkHealthCheckException(
                f"REST API request failed: {str(e)}"
            ) from e

    def execute(self, context):
        self.log.info("Starting Spark cluster health check...")
        spark = None
        
        try:
            if self.min_workers > 0:
                self._check_worker_availability()
            conf = SparkConf() \
                .setAppName(self.app_name) \
                .setMaster(self.master_url) \
                .set("spark.executor.memory", "512m") \
                .set("spark.cores.max", "1")

            spark = SparkSession.builder \
                .config(conf=conf) \
                .getOrCreate()

            test_data = [1, 2, 3, 4, 5]
            rdd = spark.sparkContext.parallelize(test_data)
            
            if rdd.count() != len(test_data):
                raise SparkHealthCheckException("RDD validation failed")
            
            self.log.info("Spark cluster health check passed")

        except Exception as e:
            self.log.error("Spark health check failed", exc_info=True)
            raise SparkHealthCheckException(f"Health check failed: {str(e)}") from e
            
        finally:
            if spark:
                spark.stop()
                self.log.info("Spark session terminated")