"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone, applies transformations, writes CSV to gold zone.

Pattern: ./data/landing/*.json -> (This Job) -> ./data/gold/
"""

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import time

# from pyspark.sql.window import Window
from .spark_session_factory import *
import os

load_dotenv()

def raw_to_bronze():
    spark = get_or_create_session()
    df = spark.read.json(f"s3a://{os.getenv('MINIO_HISTORICAL_DATA_BUCKET')}/")
    df.show()

def bronze_to_silver():
    pass

def silver_to_gold():
    pass

if __name__ == "__main__":
    raw_to_bronze()
    time.sleep(60)
    bronze_to_silver()
    time.sleep(60)
    silver_to_gold()
