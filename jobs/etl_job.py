"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone, applies transformations, writes CSV to gold zone.

Pattern: ./data/landing/*.json -> (This Job) -> ./data/gold/
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session_factory import *
import os


def run_etl(spark: SparkSession, input_path: str, output_path: str):
    """
    Main ETL pipeline: read -> transform -> write.
    
    Args:
        spark: Active SparkSession
        input_path: Landing zone path (e.g., '/opt/spark-data/landing/*.json')
        output_path: Gold zone path (e.g., '/opt/spark-data/gold')
    """
    # TODO: Implement
    df = spark.read.option("multiline", "true").json(input_path)
    
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    


if __name__ == "__main__":
    # TODO: Create SparkSession, parse args, run ETL
    # spark = create_spark_session(app_name="StreamFlowETLJob")
    spark = (SparkSession.builder.config("spark.hadoop.home.dir", "C:/hadoop").getOrCreate())
    input_path = r"C:\Users\armin\OneDrive\Desktop\Developer\StreamAnalyticsPlatform\data\landing\testdata.json"
    output_path = r"C:\Users\armin\OneDrive\Desktop\Developer\StreamAnalyticsPlatform\data\gold"
    run_etl(spark, input_path, output_path)
