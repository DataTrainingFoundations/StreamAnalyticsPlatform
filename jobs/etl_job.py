"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone and applies transformation from raw -> bronze -> silver -> gold
"""

import re
import os
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)
import boto3
from .spark_session_factory import get_or_create_session

load_dotenv()

docker_env = os.getenv("DOCKER_ENV")
dev = os.getenv("DEV")
streamflow_bucket = os.getenv("STREAMFLOW_BUCKET")

airnow_schema = StructType(
    [
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField(
            "UTC", StringType(), True
        ),  # you can convert to TimestampType later if needed
        StructField("Parameter", StringType(), True),
        StructField("Unit", StringType(), True),
        StructField("AQI", IntegerType(), True),
        StructField("Category", IntegerType(), True),
        StructField("SiteName", StringType(), True),
        StructField("AgencyName", StringType(), True),
        StructField("FullAQSCode", StringType(), True),
        StructField("IntlAQSCode", StringType(), True),
    ]
)


def process_date(paths, spark):
    """
    Processes a single date partition by reading JSON files,
    extracting date/hour from path,
    and writing to bronze layer partitioned by date/hour.
    Args:
        paths (str): S3 paths to the date partitions
            (e.g., s3a://bucket/landing/airnow/date=2026-03-10/)
        spark (SparkSession): Active Spark session for processing
    """
    df = spark.read.schema(airnow_schema).option("mode", "PERMISSIVE").json(paths)
    file_col = F.input_file_name()
    df = df.withColumn(
        "date", F.regexp_extract(file_col, r"date=(\d{4}-\d{2}-\d{2})", 1)
    ).withColumn("hour", F.regexp_extract(file_col, r"hour=(\d{2})", 1))
    # Parallelize instead of bottleneck
    df = df.repartition("date", "hour")
    (
        df.write.mode("append")
        .partitionBy("date", "hour")
        .parquet(f"s3a://{streamflow_bucket}/bronze/airnow/")
    )


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

    base_path = f"s3a://{streamflow_bucket}/landing/airnow/"

    uri = spark._jvm.java.net.URI(base_path)  # type: ignore

    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(  # type: ignore
        uri,  # type: ignore
        spark._jsc.hadoopConfiguration(),  # type: ignore
    )

    path = spark._jvm.org.apache.hadoop.fs.Path(base_path)  # type: ignore
    status = hadoop_fs.listStatus(path)  # type: ignore

    if dev == "1":
        print("Reading landing zone...")
    # Read JSON data from the landing zone in S3
    for date_status in status:
        date_path = date_status.getPath().toString()

        # skip non-date folders if needed
        if "date=" not in date_path:
            continue

        hour_statuses = hadoop_fs.listStatus(date_status.getPath())  # type: ignore

        hour_paths = []

        for hour_status in hour_statuses:
            hour_path = hour_status.getPath().toString()
            if "hour=" in hour_path:
                hour_paths.append(hour_path)

        if hour_paths:
            process_date(hour_paths, spark)
            if dev == "1":
                print(f"Processed date partition: {date_path}")


def get_partition_dates(prefix):
    """
    Returns set of partition dates like {'2026-03-10', '2026-03-11'}
    from prefixes like dt=YYYY-MM-DD
    """

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_USER"),
        aws_secret_access_key=os.getenv("AWS_PASSWORD"),
        region_name="us-east-1",
    )
    paginator = s3_client.get_paginator("list_objects_v2")
    dates = set()
    for page in paginator.paginate(
        Bucket=streamflow_bucket, Prefix=prefix, Delimiter="/"
    ):
        for p in page.get("CommonPrefixes", []):
            match = re.search(r"date=(\d{4}-\d{2}-\d{2})", p["Prefix"])
            if match:
                dates.add(match.group(1))

    return dates


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

    bronze_dates = get_partition_dates("bronze/airnow/")
    silver_dates = get_partition_dates("silver/airnow_clean/")

    dates_to_process = bronze_dates - silver_dates

    if not dates_to_process:
        print("No new data to process.")
        return

    spark = get_or_create_session()

    # Build paths list (batch read)
    paths = [
        f"s3a://{streamflow_bucket}/bronze/airnow/date={date}"
        for date in dates_to_process
    ]

    if dev == "1":
        print(f"Processing {len(paths)} date partitions")

    df = spark.read.option(
        "basePath", f"s3a://{streamflow_bucket}/bronze/airnow/"
    ).parquet(*paths)

    # Drop unnecessary columns (single call)
    df = df.drop("FullAQSCode", "UTC")

    # Filter invalid AQI
    df = df.filter(F.col("AQI") != -999)

    # Add concern level (faster mapping)
    concern_map = F.create_map(
        [
            F.lit(1),
            F.lit("Good"),
            F.lit(2),
            F.lit("Moderate"),
            F.lit(3),
            F.lit("Unhealthy for Sensitive Groups"),
            F.lit(4),
            F.lit("Unhealthy"),
            F.lit(5),
            F.lit("Very Unhealthy"),
            F.lit(6),
            F.lit("Hazardous"),
        ]
    )

    df = df.withColumn("concern_level", concern_map[F.col("Category")])

    # Deduplicate WITHOUT composite column
    df = df.dropDuplicates(["date", "hour", "IntlAQSCode", "Parameter"])

    # Normalize column names
    df = df.toDF(*[c.lower() for c in df.columns])

    # Repartition for efficient write
    df = df.repartition("date", "hour")

    # Single write (BIG performance win)
    (
        df.write.mode("append")
        .partitionBy("date", "hour")
        .parquet(f"s3a://{streamflow_bucket}/silver/airnow_clean/")
    )


def silver_to_gold():
    """
    Transform silver layer data to gold layer by aggregating into star schema.
    This function performs the following operations:
        - Reads silver data for new dates that haven't been processed into gold yet
        - Generates surrogate keys for dimensions
        - Creates dimension tables (dim_site, dim_parameter, dim_date, dim_category)
        - Creates fact table (fact_air_quality_readings) with foreign keys to dimensions
        - Writes dimension and fact tables to gold layer in Parquet format, partitioned by date where appropriate
    """
    if dev == "1":
        print("\n\nRunning gold transformation\n\n")

    spark = get_or_create_session()

    # Incremental processing
    silver_dates = get_partition_dates("silver/airnow_clean/")
    gold_dates = get_partition_dates("gold/fact_air_quality_readings/")

    dates_to_process = silver_dates - gold_dates

    if not dates_to_process:
        print("No new data for gold layer.")
        return

    paths = [
        f"s3a://{streamflow_bucket}/silver/airnow_clean/date={date}"
        for date in dates_to_process
    ]

    if dev == "1":
        print(f"Processing {len(paths)} date partitions")

    df = (
        spark.read
        .option("basePath", f"s3a://{streamflow_bucket}/silver/airnow_clean/")
        .parquet(*paths)
    )

    # ------------------------------------------------------------------ #
    # 🔑 Generate keys ONCE (reuse everywhere)
    # ------------------------------------------------------------------ #
    df = (
        df.withColumn("site_key", F.md5(F.col("intlaqscode")))
        .withColumn(
            "parameter_key",
            F.md5(F.concat_ws("_", F.col("parameter"), F.col("unit")))
        )
        .withColumn(
            "date_key",
            F.md5(F.concat_ws("_", F.col("date"), F.col("hour")))
        )
        .withColumn(
            "category_key",
            F.md5(F.col("category").cast("string"))
        )
    )

    # ------------------------------------------------------------------ #
    # DIMENSIONS (deduplicated)
    # ------------------------------------------------------------------ #

    dim_site = df.select(
        "site_key", "intlaqscode", "sitename", "agencyname", "latitude", "longitude"
    ).dropDuplicates(["site_key"])

    dim_parameter = df.select(
        "parameter_key", "parameter", "unit"
    ).dropDuplicates(["parameter_key"])

    # Add day of week (faster mapping)
    day_of_week_map = F.create_map(
        [
            F.lit(1), F.lit("Sunday"),
            F.lit(2), F.lit("Monday"),
            F.lit(3), F.lit("Tuesday"),
            F.lit(4), F.lit("Wednesday"),
            F.lit(5), F.lit("Thursday"),
            F.lit(6), F.lit("Friday"),
            F.lit(7), F.lit("Saturday"),
        ]
    )

    dim_date = (
        df.select("date_key", "date", "hour")
        .dropDuplicates(["date_key"])
        .withColumn("year", F.year(F.to_date("date")))
        .withColumn("month", F.month(F.to_date("date")))
        .withColumn("day", F.dayofmonth(F.to_date("date")))
        .withColumn("day_of_week", F.dayofweek(F.to_date("date")))
        .withColumn("day_of_week_name", day_of_week_map[F.col("day_of_week")])
    )

    dim_category = df.select(
        "category_key", "category", "concern_level"
    ).dropDuplicates(["category_key"])

    # ------------------------------------------------------------------ #
    # FACT TABLE
    # ------------------------------------------------------------------ #
    fact_table = df.select(
        F.md5(
            F.concat_ws(
                "_",
                F.col("date"),
                F.col("hour"),
                F.col("intlaqscode"),
                F.col("parameter"),
            )
        ).alias("fact_id"),
        "date",
        "hour",
        "site_key",
        "parameter_key",
        "date_key",
        "category_key",
        "aqi",
    )

    # ------------------------------------------------------------------ #
    # PERFORMANCE: repartition before writes
    # ------------------------------------------------------------------ #
    dim_site = dim_site.repartition(1)
    dim_parameter = dim_parameter.repartition(1)
    dim_category = dim_category.repartition(1)
    dim_date = dim_date.repartition("year", "month")

    fact_table = fact_table.repartition("date_key")

    # ------------------------------------------------------------------ #
    # WRITE (append but deduplicated)
    # ------------------------------------------------------------------ #
    dim_site.write.mode("append").parquet(
        f"s3a://{streamflow_bucket}/gold/airnow/dim_site/"
    )

    dim_parameter.write.mode("append").parquet(
        f"s3a://{streamflow_bucket}/gold/airnow/dim_parameter/"
    )

    dim_date.write.mode("append").partitionBy("year", "month").parquet(
        f"s3a://{streamflow_bucket}/gold/airnow/dim_date/"
    )

    dim_category.write.mode("append").parquet(
        f"s3a://{streamflow_bucket}/gold/airnow/dim_category/"
    )

    fact_table.write.mode("append").partitionBy("date", "hour").parquet(
        f"s3a://{streamflow_bucket}/gold/airnow/fact_air_quality_readings/"
    )


if __name__ == "__main__":
    # Execute the ETL pipeline sequentially with optional delays between stages
    # Run raw to bronze transformation
    # raw_to_bronze()
    # Optional: Wait 60 seconds before next transformation (commented out)
    # time.sleep(60)
    # Run bronze to silver transformation
    # bronze_to_silver()
    # Optional: Wait 60 seconds before next transformation (commented out)
    # time.sleep(60)
    # Run silver to gold transformation
    silver_to_gold()
