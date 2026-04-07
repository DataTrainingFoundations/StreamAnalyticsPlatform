"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Data Producer -> Kafka Ingest -> Spark ETL -> Validation
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.models import DagModel
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.session import provide_session
from util import constants
from scripts.airnow_raw_producers import (
    get_oldest_record_date,
    get_times,
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

def decide_ingestion(**context):
    """
    Determine whether to continue data ingestion based on the oldest record date.

    This function retrieves the oldest record date from the data source and 
    compares it to a target date.
    If the oldest record is older than or equal to the target date, the pipeline 
    is stopped; otherwise, data ingestion continues.
    The oldest date is pushed to XCom for downstream tasks.

    Args:
        **context: Airflow context dictionary, expected to contain 'ti' 
        (TaskInstance) for XCom operations.

    Returns:
        str: "stop_pipeline" if the oldest record date is less than or 
            equal to the target date, "ingest_data" otherwise.
    """
    oldest = get_oldest_record_date()

    if oldest:
        # push value to XCom
        context["ti"].xcom_push(key="oldest_date", value=oldest)
        if oldest <= datetime.strptime(constants.TARGET_DATE, constants.AIRNOW_UTC_DATE_FORMAT):
            return "stop_pipeline"
    return "produce_raw_data_to_kafka"

def produce_historical_data(**context):
    """
    Execute the AirNow data producer to fetch and publish historical air quality data.

    This function fetches historical air quality data for the previous two weeks from multiple
    bounding boxes and publishes the records to Kafka.
    It's designed to be called as an Airflow task.

    The function will raise an exception if any bounding box fails to process,
    causing the Airflow task to fail and potentially retry.

    Environment Variables Required:
        - Constants from util.constants (BBOXES list)

    Raises:
        Exception: If data fetching or publishing fails for any bounding box.
    """
    ti = context["ti"]
    oldest_date = ti.xcom_pull(
        task_ids="branch_decision",
        key="oldest_date"
    )
    start, end = get_times(oldest_date)
    run_producer(start, end)

def consumer_historical_data():
    """
    Create kafka consumer and consume data
    """
    kafka_consumer = get_consumer(os.getenv("RAW_HISTORIC_DATA_KAFKA_TOPIC", ""))
    consume_data(kafka_consumer)

@provide_session
def pause_this_dag(dag_id, session=None):
    """
    Pauses DAG
    """
    this_dag = session.query(DagModel).filter( # type: ignore
        DagModel.dag_id == dag_id
    ).first()

    this_dag.is_paused = True

# def archive_raw_historic_data():
    # """
    # Archives processed raw historic airnow data
    # """
    # archive_prefix = os.getenv("STREAMFLOW_BUCKET_ARCHIVE_PREFIX")
    # landing_prefix = os.getenv("STREAMFLOW_BUCKET_LANDING_PREFIX")
    # if not archive_prefix:
    #     raise ValueError("Missing archive prefix value")
    # if not landing_prefix:
    #     raise ValueError("Missing landing prefix value")
    # move_processed_data(landing_prefix, archive_prefix)

# Default arguments for all tasks in the DAG
default_args = {
    "owner": "Streamflow Project Team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Define the DAG with its configuration
with DAG(
    dag_id="streamflow_historic",
    default_args=default_args,
    description="StreamFlow historic airnow data pipeline: produce -> consume",
    start_date=datetime(2026, 3, 18, tzinfo=ZoneInfo('America/New_York')),
    schedule="0 19-23,0-6,9-14 * * *",
    max_active_runs=1,
    catchup=False,  # Don't run for past dates
    tags=["streamflow", "etl"],
) as dag:

    # Task 1: Check oldest date in warehouse
    check_date = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=decide_ingestion,
    )

    # Task 2a: Produce raw data from AirNow API to Kafka if oldest date is acceptable
    produce_raw_data = PythonOperator(
        task_id="produce_raw_data_to_kafka",
        python_callable=produce_historical_data,
        doc="Fetch historical air quality data from AirNow API and publish to Kafka",
    )

    # Task 2b: Stop DAG if oldest date is at the limit for our desired data
    stop = PythonOperator(
        task_id="stop_pipeline",
        python_callable=lambda: pause_this_dag("streamflow_historic"),
        doc="Pause the DAG once all desired historical data has been ingested"
    )

    # Task 3: Consume Kafka messages and write to landing zone (MinIO)
    ingest_to_landing = PythonOperator(
        task_id="ingest_raw_data_to_warehouse",
        python_callable=consumer_historical_data,
        doc="Consume from Kafka topic and write batch to MinIO landing zone",
    )

    # Task 4: Use Spark to read raw data and transform to bronze (json -> parquet)
    # transform_raw_to_bronze = PythonOperator(
    #     task_id="transform_raw_to_bronze",
    #     python_callable=raw_to_bronze,
    #     doc="Transform landing zone data and write to bronze zone",
    # )

    # Task 5: Use Spark to read bronze data and transform to silver (clean data)
    # transform_bronze_to_silver = PythonOperator(
    #     task_id="transform_bronze_to_silver",
    #     python_callable=bronze_to_silver,
    #     doc="Transform bronze zone data and write to silver zone",
    # )

    # Task 6: Use Spark to read silver data and transform to gold (star schema)
    # transform_silver_to_gold = PythonOperator(
    #     task_id="transform_silver_to_gold",
    #     python_callable=silver_to_gold,
    #     doc="Transform silver zone data and write to gold zone",
    # )

    # Task 7: Archive processed raw data
    # archive_raw_data = PythonOperator(
    #     task_id="archive_raw_data",
    #     python_callable=archive_raw_historic_data,
    #     doc="Archive processed raw historic data"
    # )

    # Set task dependencies: execute tasks sequentially

    check_date >> [produce_raw_data, stop]

    produce_raw_data >> \
        ingest_to_landing #>> \
            # transform_raw_to_bronze >> \
            #     transform_bronze_to_silver >> \
            #         transform_silver_to_gold >> \
            #             archive_raw_data
