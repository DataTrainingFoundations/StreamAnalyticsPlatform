"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone, applies transformations, writes CSV to gold zone.

Pattern: ./data/landing/*.json -> (This Job) -> ./data/gold/
"""
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
# from pyspark.sql.window import Window
from .spark_session_factory import *
import os
from util import constants
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, TimestampType


load_dotenv()

def ingest_kafka_to_silver(spark: SparkSession):
    schema = StructType([
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("UTC", StringType(), True),  # Or TimestampType() if you want automatic parsing
        StructField("Parameter", StringType(), True),
        StructField("Unit", StringType(), True),
        StructField("AQI", IntegerType(), True),
        StructField("Category", IntegerType(), True),
        StructField("SiteName", StringType(), True),
        StructField("AgencyName", StringType(), True),
        StructField("FullAQSCode", StringType(), True),
        StructField("IntlAQSCode", StringType(), True)
    ])
    df = spark.read.schema(schema).json(f"s3a://{constants.MINIO_HISTORICAL_DATA_BUCKET}/*.json")
    # df = df.filter(F.col("_corrupt_record").isNull())
    df.show()

    
def clean_data(spark: SparkSession, df: DataFrame):
    cleaned_data = df.withColumn("concern_level", \
                                F.when(df.category == 1, "Good")
                                .when(df.category == 2, "Moderate")
                                .when(df.category == 3, "Unhealthy for Sensitive Groups")
                                .when(df.category == 4, "Unhealthy")
                                .when(df.category == 5, "Very Unhealthy")
                                .when(df.category == 6, "Hazardous")
                                .otherwise(None))
    cleaned_data = cleaned_data.drop("category")          

def run_etl(spark: SparkSession, input_path: str, output_path: str):
    """
    Main ETL pipeline: read -> transform -> write.
    
    Args:
        spark: Active SparkSession
        input_path: Landing zone path (e.g., '/opt/spark-data/landing/*.json')
        output_path: Gold zone path (e.g., '/opt/spark-data/gold')
    """
    # TODO: Implement
    # df = spark.read.option("multiline", "true").json(input_path)
    
    # df.coalesce(1).write.mode("overwrite").parquet(output_path)

    # df2 = spark.read.parquet(output_path)
    # df2.show()
    ingest_kafka_to_silver(spark)
    


if __name__ == "__main__":
    # TODO: Create SparkSession, parse args, run ETL
    spark = get_or_create_session()
    input_path = r"/home/armin/project2/StreamAnalyticsPlatform/data/landing/testdata2.json"
    output_path = r"/home/armin/project2/StreamAnalyticsPlatform/data/gold"

    run_etl(spark, input_path, output_path)
