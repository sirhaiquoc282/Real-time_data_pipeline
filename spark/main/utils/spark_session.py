"""
Spark Session Management Module

Provides utilities for creating and managing Spark sessions for the ETL pipeline.
Handles Spark session initialization with appropriate configurations.
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """
    Create or retrieve existing Spark session with specified application name.
    
    Args:
        app_name (str): Name of the Spark application for identification
        
    Returns:
        SparkSession: Configured Spark session instance
        
    Usage:
        spark = create_spark_session("ProductViewsETL")
        df = spark.read.format("kafka").load()
    """
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()  # Returns existing session if available