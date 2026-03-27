"""
Helper Functions and Kafka Producers for Raw AirNow Data

This module provides functionality to fetch air quality data from the AirNow API
and publish it to a Kafka topic for downstream processing.
"""

import os
import json
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
from kafka import KafkaProducer
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from util import constants

load_dotenv()

docker_env = os.getenv("DOCKER_ENV")
dev = os.getenv("DEV")


def get_oldest_record_date():
    """
    Checks S3 bucket for the oldest date available in the data warehouse.

    Args:
        bucket_name (str): Name of the S3 bucket containing historical data.
        prefix (str): Path prefix to look for (default: 'landing/airnow/').
        s3_client (boto3.client, optional): Preconfigured boto3 S3 client.

    Returns:
        datetime or None: Oldest date found, or None if bucket is empty.
    """
    dev = os.getenv("DEV")
    streamflow_bucket = os.getenv("STREAMFLOW_BUCKET")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_USER"),
        aws_secret_access_key=os.getenv("AWS_PASSWORD"),
        region_name=os.getenv("AWS_REGION_NAME"),
    )

    try:
        progress_key = os.getenv("STREAMFLOW_BUCKET_PROGRESS_KEY")
        if progress_key:
            response = s3_client.get_object(Bucket=streamflow_bucket, Key=progress_key)
            data = json.loads(response["Body"].read())
            if dev == "1":
                print("Returning oldest date from db")
            return datetime.strptime(
                data["oldest_loaded_date"], constants.AIRNOW_UTC_DATE_FORMAT
            )
        else:
            raise ValueError("Missing streamflow bucket progress key value")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ["NoSuchKey", "NoSuchBucket"]:
            if dev == "1":
                print("Returning None")
            return None
        raise  # re-raise unexpected errors
    except ValueError as e:
        raise RuntimeError("Unable to get oldest date from warehouse") from e


def get_times(oldest_date_time: datetime | None = None):
    """
    Returns start and end datetimes (as strings) for fetching 2 weeks of historical data.

    If oldest_date_time is None, checks the warehouse to find the oldest record.
    If no records exist, defaults to the previous month from today.

    Args:
        oldest_date_time (datetime, optional): The oldest date to consider for fetching data.

    Returns:
        tuple: (start_str, end_str) where both are strings in "YYYY-MM-DDTHH" format
    """
    dev = os.getenv("DEV")

    if oldest_date_time is None:
        # Check MinIO for the oldest record
        oldest_date_time = get_oldest_record_date()

    if oldest_date_time is None:
        # Default: previous 2 weeks relative to today
        today = datetime.now()
        start_dt = today.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - relativedelta(weeks=2)
        end_dt = today.replace(hour=23, minute=0, second=0, microsecond=0) - timedelta(
            days=1
        )
        if dev == "1":
            print("Using default dates")
    else:
        # Compute full month before oldest_date_time
        oldest = oldest_date_time.replace(minute=0, second=0, microsecond=0)
        start_dt = oldest - relativedelta(weeks=2)
        end_dt = oldest - timedelta(hours=1)
        if dev == "1":
            print("Using calculated dates based on oldest time")

    start_str = start_dt.strftime(constants.AIRNOW_UTC_DATE_FORMAT)
    end_str = end_dt.strftime(constants.AIRNOW_UTC_DATE_FORMAT)
    return start_str, end_str


def fetch_historic_data(start, end, bbox, retries=3):
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
    dev = os.getenv("DEV")
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
    if dev == "1":
        print("Returning fetched airnow data")
    for i in range(retries):
        try:
            response = requests.get(airnow_url, params=params, timeout=600)
            response.raise_for_status()

            data = response.json()

            # Case 1: API returned an error object (dict)
            if isinstance(data, dict):
                if "WebServiceError" in data:
                    print(f"\n\nFailed at {bbox} for time period {start} - {end}\n\n")
                    print(
                        f"""Received this response from AirNow: {
                            data['WebServiceError'][0]['Message']
                        }\n\n"""
                    )
                    return []
                else:
                    # Unexpected dict shape
                    raise RuntimeError(f"Unexpected response format: {data}")
            # Case 2: API returned normal data (list)
            if isinstance(data, list):
                return data
            # Case 3: Something weird
            raise RuntimeError(f"Unexpected response type: {type(data)}")
        except requests.exceptions.RequestException as e:
            if i == retries - 1:
                raise RuntimeError(
                    f"Could not fetch data after {retries} attempts"
                ) from e
            time.sleep(2**i)
        except json.JSONDecodeError:
            return []
        except Exception as e:
            raise RuntimeError("An unknown error occurred", e) from e


def publish_raw_historical_records(records: list, kafka_topic: str):
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
    docker_env = os.getenv("DOCKER_ENV")
    dev = os.getenv("DEV")

    if not kafka_topic:
        raise ValueError("Missing kafka_topic parameter")

    bootstrap_server = (
        os.getenv("DOCKER_KAFKA_BOOTSTRAP_SERVER")
        if docker_env == "1"
        else os.getenv("LOCAL_KAFKA_BOOTSTRAP_SERVER")
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        acks=os.getenv("KAFKA_PRODUCER_ACKS", "all"),
        enable_idempotence=os.getenv("KAFKA_PRODUCER_ENABLE_IDEMPOTENCE", "1") == "1",
        retries=int(os.getenv("KAFKA_PRODUCER_RETRIES", "10")),
        linger_ms=int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "25")),
        batch_size=int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", "65536")),
    )
    for record in records:
        producer.send(
            kafka_topic,
            key=f"{record['IntlAQSCode']}_{record['Parameter']}",
            value=record,
        )

    producer.flush()
    if dev == "1":
        print("Batch sent.")


def fetch_current_data(bbox):
    """
    Fetches current air quality data from the AirNow API for the current hour and bounding box.

    Args:
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
        "parameters": constants.POLLUTANTS,
        "BBOX": bbox,
        "dataType": "A",
        "format": "application/json",
        "verbose": 1,
        "API_KEY": api_key,
    }
    if dev == "1":
        print("Returning fetched airnow data")
    return requests.get(airnow_url, params=params, timeout=300).json()


def run_historic_producer():
    """
    Main function for running the producer locally.

    Fetches historical data for the previous month across all bounding boxes
    and publishes to Kafka. This is primarily for testing and development.
    """
    dev = os.getenv("DEV")

    start, end = get_times()
    for bbox in constants.BBOXES:
        try:
            records = fetch_historic_data(start, end, bbox)
            publish_raw_historical_records(
                records, os.getenv("RAW_HISTORIC_DATA_KAFKA_TOPIC", "")
            )
        except Exception as e:
            print(f"Failed at {bbox} for time period {start} - {end}")
            print("Failure due to the following error:\n", e)


def run_current_producer():
    """
    Main function for running the current producer locally.

    Fetches current hour data across all bounding boxes
    and publishes to Kafka. This is primarily for testing and development.
    """

    for bbox in constants.BBOXES:
        try:
            records = fetch_current_data(bbox)
            publish_raw_historical_records(
                records, os.getenv("RAW_CURRENT_DATA_KAFKA_TOPIC", "")
            )
        except Exception as e:
            print(f"Failed at {bbox} for time period {datetime.now()}")
            print("Failure due to the following error:\n", e)


if __name__ == "__main__":
    while True:
        choice = input(
            """
            \n\nSelect which producer to run:
                '1': Historic
                '2': Current\n\n 
            """
        )
        match choice:
            case "1":
                run_historic_producer()
                break
            case "2":
                run_current_producer()
                break
            case _:
                print("Invalid input. Please choose from the options below:")
