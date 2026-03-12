"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Data Producer -> Kafka Ingest -> Spark ETL -> Validation
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from util import constants
from scripts.airnow_raw_producers import (
    fetch_cur_data,
    publish_raw_historical_records
)
from scripts.ingest_kafka_to_landing import (
    consume_historical_data
)
from jobs.etl_job import (
    raw_to_bronze,
    bronze_to_silver,
    silver_to_gold
)

# Default arguments for all tasks in the DAG
default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def produce_historical_data():
    """
    Execute the AirNow data producer to fetch and publish historical air quality data.

    This function fetches historical air quality data for the previous month from multiple
    bounding boxes (limited to first 5 for testing) and publishes the records to Kafka.
    It's designed to be called as an Airflow task.

    The function will raise an exception if any bounding box fails to process,
    causing the Airflow task to fail and potentially retry.

    Environment Variables Required:
        - Constants from util.constants (BBOXES list)

    Raises:
        Exception: If data fetching or publishing fails for any bounding box.
    """
    start, end = get_times()
    i = 0
    for bbox in constants.BBOXES:
        try:
            if i == 5:
                break
            records = fetch_month_data(start, end, bbox)
            publish_raw_historical_records(records)
            print(f"✓ Published {len(records)} records to Kafka")
            i += 1
        except Exception as e:
            print(
                f"✗ Producer failed at {bbox} for time period {start} - {end}: {str(e)}"
            )
            raise


# Define the DAG with its configuration
with DAG(
    dag_id="streamflow_historic",
    default_args=default_args,
    description="StreamFlow data pipeline: produce -> consume -> transform",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,  # Don't run for past dates
    tags=["streamflow", "etl"],
) as dag:

    # Task 1: Produce raw data from AirNow API to Kafka
    produce_raw_data = PythonOperator(
        task_id="produce_raw_data_to_kafka",
        python_callable=produce_historical_data,
        doc="Fetch historical air quality data from AirNow API and publish to Kafka",
    )

    # Task 2: Consume Kafka messages and write to landing zone (MinIO)
    ingest_to_landing = PythonOperator(
        task_id="ingest_raw_data_to_warehouse",
        python_callable=consume_historical_data,
        doc="Consume from Kafka topic and write batch to MinIO landing zone",
    )

    # Task 3: Use Spark to read raw data and transform to bronze (json -> parquet)
    transform_raw_to_bronze = PythonOperator(
        task_id="transform_raw_to_bronze",
        python_callable=raw_to_bronze,
        doc="Transform landing zone data and write to bronze zone",
    )

    # Task 4: Use Spark to read bronze data and transform to silver (clean data)
    transform_bronze_to_silver = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=bronze_to_silver,
        doc="Transform bronze zone data and write to silver zone",
    )

    # Task 5: Use Spark to read silver data and transform to gold (star schema)
    transform_silver_to_gold = PythonOperator(
        task_id="transform_silver_to_gold",
        python_callable=silver_to_gold,
        doc="Transform silver zone data and write to gold zone",
    )

    # Set task dependencies: execute tasks sequentially
    produce_raw_data >> \
        ingest_to_landing >> \
            transform_raw_to_bronze >> \
                transform_bronze_to_silver >> \
                    transform_silver_to_gold
