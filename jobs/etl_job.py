"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone and applies transformation from raw -> bronze -> silver -> gold
"""

import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# from pyspark.sql.window import Window
from .spark_session_factory import get_or_create_session

def raw_to_bronze():
    """
    Transform raw JSON data from the landing zone to the bronze layer.

    Reads JSON files from the S3 landing zone, partitions by date and hour,
    and writes them as Parquet files to the bronze zone for further processing.

    Raises:
        Exception: If Spark session creation or data reading/writing fails.
    """
    spark = get_or_create_session()
    df = spark.read.json("s3a://streamflow-data/landing/airnow/")
    df.write.mode("append").partitionBy("date").parquet(
        "s3a://streamflow-data/bronze/airnow/"
    )


def bronze_to_silver():
    """
    Transform bronze layer data to silver layer by cleaning and standardizing.

    This function is a placeholder for data cleaning operations such as:
    - Removing duplicates
    - Handling missing values
    - Standardizing data types
    - Filtering invalid records

    Currently not implemented (TODO).

    TODO: Implement silver transformation logic.
    """
    spark = get_or_create_session()
    clean_df = spark.read.parquet("s3a://streamflow-data/bronze/airnow")
    # .parquet("s3a://streamflow-data/silver/airnow_clean/")
    clean_df = clean_df.withColumn("concern_level", \
                                F.when(clean_df.Category == 1, "Good")
                                .when(clean_df.Category == 2, "Moderate")
                                .when(clean_df.Category == 3, "Unhealthy for Sensitive Groups")
                                .when(clean_df.Category == 4, "Unhealthy")
                                .when(clean_df.Category == 5, "Very Unhealthy")
                                .when(clean_df.Category == 6, "Hazardous")
                                .otherwise(None))
    clean_df = clean_df.drop("Category")
    clean_df = clean_df.drop("FullAQSCode")
    clean_df = clean_df.drop("UTC")
    clean_df = clean_df.drop("ingested_at")
    clean_df = clean_df.withColumn("composite_key",
                                   F.concat_ws("_", F.col("date"), F.col("hour"), F.col("IntlAQSCode"), F.col("Parameter")))
    clean_df = clean_df.drop_duplicates(["composite_key"])
    clean_df = clean_df.toDF(*[c.lower() for c in clean_df.columns])
    clean_df.write.mode("append").partitionBy("date").parquet("s3a://streamflow-data/silver/airnow_clean/")


def silver_to_gold():
    """
    Transform silver layer data to gold layer by aggregating into star schema.

    This function is a placeholder for creating fact and dimension tables:
    - Creating air quality fact table
    - Building dimension tables (e.g., location, time, pollutant)
    - Aggregating metrics for analytics

    Currently not implemented (TODO).

    TODO: Implement gold transformation logic.
    """
    spark = get_or_create_session()
    silver_df = spark.read.parquet("s3a://streamflow-data/silver/airnow_clean")
    silver_df.show()
    # fact_df.write.mode("overwrite") \
    # .parquet("s3a://streamflow-data/gold/air_quality_fact/")


if __name__ == "__main__":
    # Execute the ETL pipeline sequentially with delays between stages
    # raw_to_bronze()
    # time.sleep(60)  # Wait 60 seconds before next transformation
    # bronze_to_silver()
    # time.sleep(60)  # Wait 60 seconds before next transformation
    silver_to_gold()
