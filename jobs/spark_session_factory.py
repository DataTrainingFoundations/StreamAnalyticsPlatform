"""
SparkSession Factory Module
jobs/spark_session_factory.py
"""

from typing import Optional
import os
import sys
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from util import *

load_dotenv()
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

spark_app_name = os.getenv("SPARK_APP_NAME", "AirNowStreamAnalytics")
spark_master = os.getenv("SPARK_MASTER", "local[*]")

def create_spark_session(
    app_name: str = spark_app_name,
    master: str = spark_master,
    config_overrides: Optional[dict] = None
) -> SparkSession:
    """
    Create and return a configured SparkSession.
    Args:
        app_name: Name of the Spark application
        master: Spark master URL (e.g., "local[*]")
        config_overrides: Optional dictionary of additional Spark configs
    """
    docker_env = os.getenv("DOCKER_ENV")

    builder = (
        SparkSession.builder
        .appName(app_name) # type: ignore
        .master(master)
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("DOCKER_MINIO_ENDPOINT")
                if docker_env == "1"
                else os.getenv("LOCAL_MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if config_overrides:
        for key, value in config_overrides.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_or_create_session(app_name: str = spark_app_name) -> SparkSession:
    """
    Return the active SparkSession or create a default one.
    Safe to call from etl_job.py without risking duplicate sessions.

    Args:
        app_name: Fallback app name if a new session must be created
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return create_spark_session(app_name)


def stop_session(spark: SparkSession) -> None:
    """
    Gracefully stop the SparkSession.
    Always call in a finally block inside etl_job.py.

    Args:
        spark: The SparkSession to stop
    """
    if spark is not None:
        spark.stop()
