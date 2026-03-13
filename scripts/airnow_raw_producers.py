"""
Producer for Raw Historical Data Kafka Topic

This module provides functionality to fetch historical air quality data from the AirNow API
and publish it to a Kafka topic for downstream processing.
"""

import os
import json
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
from kafka import KafkaProducer
import boto3
from dotenv import load_dotenv
from util import constants

load_dotenv()

docker_env = os.getenv("DOCKER_ENV")

def get_oldest_record_date(bucket_name=None, prefix="landing/airnow/", s3_client=None):
    """
    Checks S3 bucket for the oldest date available in the data warehouse.

    Args:
        bucket_name (str): Name of the S3 bucket containing historical data.
        prefix (str): Path prefix to look for (default: 'landing/airnow/').
        s3_client (boto3.client, optional): Preconfigured boto3 S3 client.

    Returns:
        datetime or None: Oldest date found, or None if bucket is empty.
    """
    if bucket_name is None:
        bucket_name = os.getenv("STREAMFLOW_BUCKET")

    if s3_client is None:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_USER"),
            aws_secret_access_key=os.getenv("AWS_PASSWORD"),
            region_name="us-east-1"
        )

    # Create bucket, if nonexistent
    existing_buckets = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    if bucket_name not in existing_buckets:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")

    # List all objects under the prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    dates = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            # Match dt=YYYY-MM-DD pattern in key
            match = re.search(r"dt=(\d{4}-\d{2}-\d{2})", obj["Key"])
            if match:
                dt_str = match.group(1)
                dt_obj = datetime.strptime(dt_str, "%Y-%m-%d")
                dates.append(dt_obj)

    if dates:
        return min(dates)
    else:
        return None  # No data found

def get_times(oldest_date_time=None):
    """
    Returns start and end datetimes (as strings) for fetching a full month of historical data.

    If oldest_date_time is None, checks the warehouse to find the oldest record.
    If no records exist, defaults to the previous month from today.

    Args:
        oldest_date_time (datetime, optional): The oldest date to consider for fetching data.

    Returns:
        tuple: (start_str, end_str) where both are strings in "YYYY-MM-DDTHH" format
    """
    if oldest_date_time is None:
        # Check MinIO for the oldest record
        oldest_date_time = get_oldest_record_date()

    if oldest_date_time is None:
        # Default: previous month relative to today
        today = datetime.now()
        start_dt = today.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        ) - relativedelta(months=1)
        end_dt = today.replace(
            day=1, hour=23, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)
    else:
        # Compute full month before oldest_date_time
        oldest = oldest_date_time.replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = oldest.replace(day=1) - relativedelta(months=1)
        end_dt = oldest.replace(day=1, hour=23) - timedelta(days=1)

    start_str = start_dt.strftime("%Y-%m-%dT%H")
    end_str = end_dt.strftime("%Y-%m-%dT%H")
    return start_str, end_str


def fetch_month_data(start, end, bbox):
    """
    Fetches historical air quality data from the AirNow API for a given time range and bounding box.

    Args:
        start (str): Start date/time in "YYYY-MM-DDTHH" format.
        end (str): End date/time in "YYYY-MM-DDTHH" format.
        bbox (str): Bounding box coordinates as a comma-separated string 
        (e.g., "lat1,lng1,lat2,lng2").

    Returns:
        list: List of air quality measurement records from the API.

    Raises:
        ValueError: If required environment variables (API key or URL) are missing.
        requests.RequestException: If the API request fails.

    Environment Variables:
        - AIRNOW_API_KEY: API key for AirNow API access
        - AIRNOW_DATA_URL: Base URL for AirNow data API
    """
    api_key = os.getenv("AIRNOW_API_KEY", "")
    airnow_url = os.getenv("AIRNOW_DATA_URL", "")

    if api_key == "":
        raise ValueError("Missing API key")
    if airnow_url == "":
        raise ValueError("Missing airnow url")

    params = {
        "startDate": start,
        "endDate": end,
        "parameters": constants.POLLUTANTS,
        "BBOX": bbox,
        "dataType": "A",
        "format": "application/json",
        "verbose": 1,
        "API_KEY": api_key,
    }
    return requests.get(airnow_url, params=params, timeout=300).json()


def publish_raw_historical_records(records):
    """
    Publishes raw historical records to the corresponding Kafka topic.

    Each record is enriched with an ingestion timestamp and published with a key
    based on the AQS code and parameter for proper partitioning.

    Args:
        records (list): List of air quality measurement records to publish.

    Environment Variables:
        - DOCKER_ENV: Determines whether to use Docker or local Kafka settings
        - DOCKER_KAFKA_BOOTSTRAP_SERVER / LOCAL_KAFKA_BOOTSTRAP_SERVER: Kafka bootstrap servers
        - RAW_HISTORIC_DATA_KAFKA_TOPIC: Target Kafka topic name

    Raises:
        kafka.KafkaError: If publishing to Kafka fails.
    """
    bootstrap_server = (
        os.getenv("DOCKER_KAFKA_BOOTSTRAP_SERVER")
        if docker_env == "1"
        else os.getenv("LOCAL_KAFKA_BOOTSTRAP_SERVER")
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    kafka_topic = os.getenv("RAW_HISTORIC_DATA_KAFKA_TOPIC")
    for record in records:
        # record["ingested_at"] = datetime.now().isoformat()
        message_key = f"{record['IntlAQSCode']}_{record['Parameter']}"
        producer.send(
            kafka_topic,
            key=message_key.encode(),
            value=record,
        )

    producer.flush()
    print("Batch sent.")


def main():
    """
    Main function for running the producer locally.

    Fetches historical data for the previous month across all bounding boxes
    and publishes to Kafka. This is primarily for testing and development.
    """
    start, end = get_times()

    for bbox in constants.BBOXES:
        try:
            records = fetch_month_data(start, end, bbox)
            publish_raw_historical_records(records)
        except Exception as e:
            print(f"Failed at {bbox} for time period {start} - {end}")
            print("Failure due to the following error:\n", e)


if __name__ == "__main__":
    main()
