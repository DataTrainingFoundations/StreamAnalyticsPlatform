"""
SparkSession Factory Module
jobs/spark_session_factory.py
"""
from pyspark.sql import SparkSession
from typing import Optional
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.constants import SPARK_APP_NAME, SPARK_MASTER


def create_spark_session(
    app_name: str = SPARK_APP_NAME,
    master: str = SPARK_MASTER,
    config_overrides: Optional[dict] = None
) -> SparkSession:
    """
    Create and return a configured SparkSession.
    Args:
        app_name: Name of the Spark application
        master: Spark master URL (e.g., "local[*]")
        config_overrides: Optional dictionary of additional Spark configs
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
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


def get_or_create_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
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
