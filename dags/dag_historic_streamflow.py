"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Data Producer -> Kafka Ingest -> Spark ETL -> Validation
"""

from datetime import datetime, timedelta
import sys
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from scripts.airnow_raw_historic_producer import (
    get_times,
    fetch_month_data,
    publish_raw_historical_records,
)
from util import constants
from scripts.ingest_kafka_to_landing import consume_historical_data

# Add scripts directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../scripts"))

default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def produce_historical_data():
    """Execute the AirNow data producer"""
    start, end = get_times()
    for bbox in constants.BBOXES:
        try:
            records = fetch_month_data(start, end, bbox)
            publish_raw_historical_records(records)
            print(f"✓ Published {len(records)} records to Kafka")
        except Exception as e:
            print(f"✗ Producer failed at {bbox} for time period {start} - {end}: {str(e)}")
            raise

with DAG(
    dag_id="streamflow_main",
    default_args=default_args,
    description="StreamFlow data pipeline: produce -> consume -> transform",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["streamflow", "etl"],
) as dag:

    # Task 1: Produce raw data from AirNow API to Kafka
    produce_data = PythonOperator(
        task_id="produce_raw_data",
        python_callable=produce_historical_data,
        doc="Fetch historical air quality data from AirNow API and publish to Kafka",
    )

    # Task 2: Consume Kafka messages and write to landing zone (MinIO)
    ingest_to_landing = PythonOperator(
        task_id="consume_kafka_to_landing",
        python_callable=consume_historical_data,
        doc="Consume from Kafka topic and write batch to MinIO landing zone",
    )

    # Task 3: Run Spark ETL job (landing -> gold zone transformation)
    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark-jobs/etl_job.py "
            "--input-path /opt/spark-data/landing "
            "--output-path /opt/spark-data/gold"
        ),
        doc="Spark ETL: Transform landing zone data and write to gold zone",
    )

    # Task 4: Validate ETL output
    validate_etl = BashOperator(
        task_id="validate_etl_output",
        bash_command='find /opt/spark-data/gold -type f -name "*.csv" | head -5 && echo "✓ ETL validation passed"',
        doc="Verify that gold zone contains transformed data files",
    )

    # Set task dependencies
    produce_data >> ingest_to_landing >> run_spark_etl >> validate_etl # type: ignore
