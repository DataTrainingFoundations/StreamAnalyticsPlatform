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
    # Create or retrieve the Spark session
    spark = get_or_create_session()
    # Read JSON data from the landing zone in S3
    df = spark.read.json("s3a://streamflow-data/landing/airnow/")
    # Write the data in Parquet format to the bronze zone, partitioned by date
    df.write.mode("append").partitionBy("date").parquet(
        "s3a://streamflow-data/bronze/airnow/"
    )


def bronze_to_silver():
    """
    Transform bronze layer data to silver layer by cleaning and standardizing.

    This function performs data cleaning operations such as:
    - Adding a concern level based on category
    - Removing unnecessary columns
    - Creating a composite key for deduplication
    - Converting column names to lowercase
    - Removing duplicates
    """
    # Create or retrieve the Spark session
    spark = get_or_create_session()
    # Read Parquet data from the bronze layer
    clean_df = spark.read.parquet("s3a://streamflow-data/bronze/airnow")
    # Add a new column 'concern_level' based on the 'Category' value
    clean_df = clean_df.withColumn("concern_level", \
                                F.when(clean_df.Category == 1, "Good")
                                .when(clean_df.Category == 2, "Moderate")
                                .when(clean_df.Category == 3, "Unhealthy for Sensitive Groups")
                                .when(clean_df.Category == 4, "Unhealthy")
                                .when(clean_df.Category == 5, "Very Unhealthy")
                                .when(clean_df.Category == 6, "Hazardous")
                                .otherwise(None))
    # Drop the original 'Category' column as it's replaced by 'concern_level'
    clean_df = clean_df.drop("Category")
    # Drop unnecessary columns: FullAQSCode, UTC, ingested_at
    clean_df = clean_df.drop("FullAQSCode")
    clean_df = clean_df.drop("UTC")
    # Create a composite key for deduplication using date, hour, IntlAQSCode, and Parameter
    clean_df = clean_df.withColumn("composite_key",
                                   F.concat_ws("_", F.col("date"), F.col("hour"), F.col("IntlAQSCode"), F.col("Parameter")))
    # Remove duplicate records based on the composite key
    clean_df = clean_df.drop_duplicates(["composite_key"])
    # Convert all column names to lowercase for consistency
    clean_df = clean_df.toDF(*[c.lower() for c in clean_df.columns])
    # Write the cleaned data to the silver layer, partitioned by date
    clean_df.write.mode("append").partitionBy("date").parquet("s3a://streamflow-data/silver/airnow_clean/")


def silver_to_gold():
    """
    Transform silver layer data to gold layer by aggregating into star schema.

    This function should create fact and dimension tables for analytics:
    - Read cleaned data from silver layer
    - Create air quality fact table with measurements
    - Build dimension tables (e.g., location, time, pollutant)
    - Aggregate metrics for reporting and analytics

    Currently a placeholder - shows the data for inspection.
    """
    # Create or retrieve the Spark session
    spark = get_or_create_session()
    # Read cleaned Parquet data from the silver layer
    silver_df = spark.read.parquet("s3a://streamflow-data/silver/airnow_clean")
    # Display the data for inspection (temporary)
    silver_df.show()
    # TODO: Implement aggregation logic to create fact and dimension tables
    # Example: Create fact table with aggregated air quality metrics
    # fact_df = silver_df.groupBy("date", "location").agg(F.avg("value").alias("avg_value"))
    # fact_df.write.mode("overwrite").parquet("s3a://streamflow-data/gold/air_quality_fact/")


if __name__ == "__main__":
    # Execute the ETL pipeline sequentially with optional delays between stages
    # Run raw to bronze transformation
    raw_to_bronze()
    # Optional: Wait 60 seconds before next transformation (commented out)
    # time.sleep(60)
    # Run bronze to silver transformation
    bronze_to_silver()
    # Optional: Wait 60 seconds before next transformation (commented out)
    # time.sleep(60)
    # Run silver to gold transformation
    silver_to_gold()
