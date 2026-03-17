"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone and applies transformation from raw -> bronze -> silver -> gold
"""

import time
import re
import os
from dotenv import load_dotenv 
from pyspark.sql import SparkSession, DataFrame, functions as F
# from pyspark.sql.window import Window
from .spark_session_factory import get_or_create_session
import boto3

load_dotenv()

docker_env = os.getenv("DOCKER_ENV")
dev = os.getenv("DEV")
streamflow_bucket = os.getenv("STREAMFLOW_BUCKET")

def get_partition_dates(prefix):
    """
    Returns set of partition dates like {'2026-03-10', '2026-03-11'}
    from prefixes like dt=YYYY-MM-DD
    """

    s3_client = (
        boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_USER"),
            aws_secret_access_key=os.getenv("AWS_PASSWORD"),
            region_name="us-east-1",
        )
        if dev != "1"
        else boto3.client(
            "s3",
            endpoint_url=(
                os.getenv("DOCKER_MINIO_ENDPOINT")
                if docker_env == "1"
                else os.getenv("LOCAL_MINIO_ENDPOINT")
            ),
            aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        )
    )
    paginator = s3_client.get_paginator("list_objects_v2")
    dates = set()
    if dev == "1":
        print("Starting date filtering")
    for page in paginator.paginate(Bucket=streamflow_bucket, Prefix=prefix, Delimiter="/"):
        for p in page.get("CommonPrefixes", []):
            match = re.search(r"date=(\d{4}-\d{2}-\d{2})", p["Prefix"])
            if match:
                dates.add(match.group(1))

    return dates

def raw_to_bronze():
    """
    Transform raw JSON data from the landing zone to the bronze layer.

    Reads JSON files from the S3 landing zone, partitions by date and hour,
    and writes them as Parquet files to the bronze zone for further processing.

    Raises:
        Exception: If Spark session creation or data reading/writing fails.
    """
    if dev == "1":
        print("\n\nRunning bronze transformation\n\n")
    # Create or retrieve the Spark session
    spark = get_or_create_session()
    # Read JSON data from the landing zone in S3
    df = spark.read.json(f"s3a://{streamflow_bucket}/landing/airnow/")
    # Write the data in Parquet format to the bronze zone, partitioned by date
    df.write.mode("append").partitionBy("date").parquet(
        f"s3a://{streamflow_bucket}/bronze/airnow/"
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
    if dev == "1":
        print("\n\nRunning silver transformation\n\n")
    # Get new date partitions to process from bronze zone
    bronze_dates = get_partition_dates("bronze/airnow/")
    if dev == '1':
        print("\nBronze Dates: ", len(bronze_dates))
    silver_dates = get_partition_dates("silver/airnow_clean/")
    if dev == '1':
        print("\nSilver Dates: ", len(silver_dates), "\n\n")
    dates_to_process = bronze_dates - silver_dates
    # Create or retrieve the Spark session
    spark = get_or_create_session()

    for date in dates_to_process:
        # Read Parquet data from the bronze layer
        clean_df = spark.read.parquet(f"s3a://{streamflow_bucket}/bronze/airnow/date={date}")
        # Add a new column 'concern_level' based on the 'Category' value
        clean_df = clean_df.filter(F.col("AQI") != -999)
        clean_df = clean_df.withColumn(
            "concern_level",
            F.when(clean_df.Category == 1, "Good")
            .when(clean_df.Category == 2, "Moderate")
            .when(clean_df.Category == 3, "Unhealthy for Sensitive Groups")
            .when(clean_df.Category == 4, "Unhealthy")
            .when(clean_df.Category == 5, "Very Unhealthy")
            .when(clean_df.Category == 6, "Hazardous")
            .otherwise(None),
        )
        # Drop the original 'Category' column as it's replaced by 'concern_level'
        clean_df = clean_df.drop("Category")
        # Drop unnecessary columns: FullAQSCode, UTC, ingested_at
        clean_df = clean_df.drop("FullAQSCode")
        clean_df = clean_df.drop("UTC")
        # Create a composite key for deduplication using date, hour, IntlAQSCode, and Parameter
        clean_df = clean_df.withColumn(
            "composite_key",
            F.concat_ws(
                "_", F.lit(date), F.col("hour"), F.col("IntlAQSCode"), F.col("Parameter")
            ),
        )
        # Remove duplicate records based on the composite key
        clean_df = clean_df.drop_duplicates(["composite_key"])
        # Convert all column names to lowercase for consistency
        clean_df = clean_df.toDF(*[c.lower() for c in clean_df.columns])
        # Write the cleaned data to the silver layer, partitioned by date
        clean_df.write.mode("append").parquet(
            f"s3a://{streamflow_bucket}/silver/airnow_clean/date={date}"
        )

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
    silver_df = spark.read.parquet("s3a://stream-analytics-project-bucket/silver/airnow_clean")
    # Display the data for inspection (temporary)
    #silver_df.show()
    # TODO: Implement aggregation logic to create fact and dimension tables
    #create table for monitoring sites
    dim_site = silver_df.select(
        "intlaqscode",
        "sitename",
        "agencyname",
        "latitude",
        "longitude"
    ).distinct() \
     .withColumn("site_key", F.md5(F.col("intlaqscode")))
    
    #create table for parameter (pollutants)
    dim_parameter = silver_df.select(
        "parameter",
        "unit"
    ).distinct() \
     .withColumn("parameter_key", F.md5(F.concat_ws("_", F.col("parameter"), F.col("unit"))))

    #create date table
    dim_date = silver_df.select(
        "date",
        "hour"
    ).distinct() \
     .withColumn("date_key", F.md5(F.concat_ws("_", F.col("date"), F.col("hour")))) \
     .withColumn("year", F.year(F.to_date(F.col("date"), "yyyy-MM-dd"))) \
     .withColumn("month", F.month(F.to_date(F.col("date"), "yyyy-MM-dd"))) \
     .withColumn("day", F.dayofmonth(F.to_date(F.col("date"), "yyyy-MM-dd"))) \
     .withColumn("day_of_week", F.dayofweek(F.to_date(F.col("date"), "yyyy-MM-dd"))) \
     .withColumn("day_name", \
                                F.when(F.col("day_of_week") == 1, "Sunday")
                                .when(F.col("day_of_week") == 2, "Monday")
                                .when(F.col("day_of_week") == 3, "Tuesday")
                                .when(F.col("day_of_week") == 4, "Wednesday")
                                .when(F.col("day_of_week") == 5, "Thursday")
                                .when(F.col("day_of_week") == 6, "Friday")
                                .when(F.col("day_of_week") == 7, "Saturday")
                                .otherwise(None))
    
    #create category and concern level table
    dim_category = silver_df.select(
        "category",
        "concern_level"
    ).distinct() \
    .withColumn("category_key", F.md5(F.col("category").cast("string")))

    # ------------------------------------------------------------------ #
    # FACT TABLE                                                           #
    # ------------------------------------------------------------------ #

    fact_table = silver_df \
        .withColumn("site_key", F.md5(F.col("intlaqscode"))) \
        .withColumn("parameter_key", F.md5(F.concat_ws("_", F.col("parameter"), F.col("unit")))) \
        .withColumn("date_key", F.md5(F.concat_ws("_", F.col("date"), F.col("hour")))) \
        .withColumn("category_key", F.md5(F.col("category").cast("string"))) \
        .withColumn("composite_key", F.md5(F.col("composite_key"))) \
        .select(
            "composite_key",
            "site_key",
            "parameter_key",
            "date_key",
            "category_key",
            "aqi",
            "ingested_at"
        )

    # ------------------------------------------------------------------ #
    # WRITE TO GOLD LAYER                                                  #
    # ------------------------------------------------------------------ #

    dim_site.write.mode("append").parquet("s3a://stream-analytics-project-bucket/gold/dim_site/")
    dim_parameter.write.mode("append").parquet("s3a://stream-analytics-project-bucket/gold/dim_parameter/")
    dim_date.write.mode("append").parquet("s3a://stream-analytics-project-bucket/gold/dim_date/")
    dim_category.write.mode("append").parquet("s3a://stream-analytics-project-bucket/gold/dim_category/")

    fact_table.write.mode("append").partitionBy("date_key").parquet(
        "s3a://stream-analytics-project-bucket/gold/fact_air_quality_readings/"
    )

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
