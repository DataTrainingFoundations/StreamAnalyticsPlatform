import os
import pytest
from jobs import spark_session_factory
from pyspark.sql import SparkSession


def test_create_spark_session_defaults_to_local(monkeypatch):
    monkeypatch.setenv("DOCKER_ENV", "0")
    monkeypatch.setenv("DEV", "0")
    # ensure environment variable that influences default master is cleared
    monkeypatch.delenv("DOCKER_SPARK_MASTER", raising=False)

    spark = spark_session_factory.create_spark_session(app_name="pytest-spark-test")
    assert isinstance(spark, SparkSession)
    assert spark.sparkContext is not None
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

    # Verify a setting from non-DOCKER path is present
    assert spark.conf.get("spark.sql.shuffle.partitions") == "8"

    spark.stop()


def test_get_or_create_session_returns_active(monkeypatch):
    monkeypatch.setenv("DOCKER_ENV", "0")
    monkeypatch.setenv("DEV", "0")
    spark = spark_session_factory.create_spark_session(app_name="pytest-spark-test2")

    got = spark_session_factory.get_or_create_session(app_name="unused")
    assert got == spark

    spark.stop()


def test_create_spark_session_docker_minio_config(monkeypatch):
    monkeypatch.setenv("DOCKER_ENV", "1")
    monkeypatch.setenv("DEV", "1")
    monkeypatch.setenv("DOCKER_MINIO_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "minioadmin")
    monkeypatch.setenv("DOCKER_SPARK_MASTER", "local[1]")

    spark = spark_session_factory.create_spark_session(app_name="pytest-minio-test")
    assert spark.conf.get("spark.hadoop.fs.s3a.endpoint") == "http://localhost:9000"

    spark.stop()
