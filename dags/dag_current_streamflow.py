"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Data Producer -> Kafka Ingest -> Spark ETL -> Validation
"""

from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.airnow_raw_producers import (
    run_producer
)
from scripts.ingest_kafka_to_landing import (
    get_consumer,
    consume_data
)
# from scripts.cleanup_data import move_processed_data
# from jobs.etl_job import (
#     raw_to_bronze,
#     bronze_to_silver,
#     silver_to_gold
# )

load_dotenv()

DEV = os.getenv("DEV", "")

def consumer_historical_data():
    """
    Create kafka consumer and consume data
    """
    kafka_consumer = get_consumer(os.getenv("RAW_CURRENT_DATA_KAFKA_TOPIC", ""))
    consume_data(kafka_consumer, is_historic=False)

# def archive_raw_current_data():
#     """
#     Archives processed raw current airnow data
#     """
#     archive_prefix = os.getenv("STREAMFLOW_BUCKET_ARCHIVE_PREFIX")
#     landing_prefix = os.getenv("STREAMFLOW_BUCKET_LANDING_PREFIX")
#     if not archive_prefix:
#         raise ValueError("Missing archive prefix value")
#     if not landing_prefix:
#         raise ValueError("Missing landing prefix value")
#     move_processed_data(landing_prefix, archive_prefix)

# Default arguments for all tasks in the DAG
default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG with its configuration
with DAG(
    dag_id="streamflow_current",
    default_args=default_args,
    description="StreamFlow current airnow data pipeline: produce -> consume",
    start_date=datetime(2026, 3, 18),
    schedule="@hourly",
    max_active_runs=1,
    catchup=False,  # Don't run for past dates
    tags=["streamflow", "etl"],
) as dag:

     # Task 1: Produce raw data from AirNow API to Kafka if oldest date is acceptable
    produce_raw_data = PythonOperator(
        task_id="produce_raw_data_to_kafka",
        python_callable=lambda: run_producer("", "", os.getenv("RAW_CURRENT_DATA_KAFKA_TOPIC", "")),
        doc="Fetch historical air quality data from AirNow API and publish to Kafka",
    )

    # Task 2: Consume Kafka messages and write to landing zone (MinIO)
    ingest_to_landing = PythonOperator(
        task_id="ingest_raw_data_to_warehouse",
        python_callable=consumer_historical_data,
        doc="Consume from Kafka topic and write batch to MinIO landing zone",
    )

    # # Task 4: Use Spark to read raw data and transform to bronze (json -> parquet)
    # transform_raw_to_bronze = PythonOperator(
    #     task_id="transform_raw_to_bronze",
    #     python_callable=raw_to_bronze,
    #     doc="Transform landing zone data and write to bronze zone",
    # )

    # # Task 5: Use Spark to read bronze data and transform to silver (clean data)
    # transform_bronze_to_silver = PythonOperator(
    #     task_id="transform_bronze_to_silver",
    #     python_callable=bronze_to_silver,
    #     doc="Transform bronze zone data and write to silver zone",
    # )

    # # Task 6: Use Spark to read silver data and transform to gold (star schema)
    # transform_silver_to_gold = PythonOperator(
    #     task_id="transform_silver_to_gold",
    #     python_callable=silver_to_gold,
    #     doc="Transform silver zone data and write to gold zone",
    # )

    # # Task 7: Archive processed raw data
    # archive_raw_data = PythonOperator(
    #     task_id="archive_raw_data",
    #     python_callable=archive_raw_current_data,
    #     doc="Archive processed raw current data"
    # )

    # Set task dependencies: execute tasks sequentially

    produce_raw_data >> \
        ingest_to_landing #>> \
            # transform_raw_to_bronze >> \
            #     transform_bronze_to_silver >> \
            #         transform_silver_to_gold >> \
            #             archive_raw_data
