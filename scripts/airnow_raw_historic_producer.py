"""
Producer for Raw Historical Data Kafka Topic
"""

import os
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
from kafka import KafkaProducer
import boto3
from dotenv import load_dotenv
from util import constants

load_dotenv()

def get_times(oldest_date_time=None):
    """
    Returns start and end datetimes (as strings) for fetching a full month of historical data.
    
    - If oldest_date_time is None, defaults to previous month from today.
    - If oldest_date_time is provided, fetches the month immediately **before** that date.
    - Output format: "YYYY-MM-DDTHH" for API compatibility.
    """
    if oldest_date_time is None:
        # Default: previous month relative to today
        today = datetime.now()
        # First day of previous month
        start_dt = (today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    - relativedelta(months=1))
        # Last day of previous month
        end_dt = (today.replace(day=1, hour=23, minute=0, second=0, microsecond=0)
                  - timedelta(days=1))
    else:
        # Compute full month before oldest_date_time
        oldest = oldest_date_time.replace(hour=0, minute=0, second=0, microsecond=0)
        # Start = first day of month before oldest
        start_dt = (oldest.replace(day=1) - relativedelta(months=1))
        # End = last day of month before oldest
        end_dt = (oldest.replace(day=1, hour=23) - timedelta(days=1))

    # Convert to API-compatible string
    start_str = start_dt.strftime("%Y-%m-%dT%H")
    end_str = end_dt.strftime("%Y-%m-%dT%H")

    return start_str, end_str

def fetch_month_data(start, end, bbox):
    """
    Fetches historical data (from yesterday to now by default)
    """
    api_key = os.getenv("AIRNOW_API_KEY", "")
    airnow_url = os.getenv("AIRNOW_HISTORIC_DATA_URL", "")

    if api_key == "":
        raise ValueError("Missing API key")
    if airnow_url == "":
        raise ValueError("Missing airnow url")

    params = {
        "startDate": start,
        "endDate": end,
        "parameters": "PM25,PM10,OZONE,NO2,CO,SO2",
        "BBOX": bbox,  
        "dataType": "A",
        "format": "application/json",
        "verbose": 1,
        "API_KEY": api_key,
    }
    return requests.get(airnow_url, params=params, timeout=300_000).json()

def publish_raw_historical_records(records):
    """
    Publishes raw historical records to corresponding kafka topic
    """
    docker_env = os.getenv("DOCKER_ENV")
    bootstrap_server = (
        os.getenv("DOCKER_KAFKA_BOOTSTRAP_SERVER")
        if docker_env == "1"
        else os.getenv("LOCAL_KAFKA_BOOTSTRAP_SERVER")
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    kafka_topic = os.getenv("RAW_HISTORICAL_DATA_KAFKA_TOPIC")
    for record in records:
        record["source"] = "airnow_historical"
        record["ingested_at"] = datetime.now().isoformat()
        message_key = f"{record['FullAQSCode']}_{record['Parameter']}_{record['ingested_at']}".encode()
        producer.send(
            kafka_topic,
            key=message_key,
            value=record,
        )

    producer.flush()
    print("Batch sent.")


def main():
    """
    Main function for running producer locally
    """
    # TODO: Add functionality to retrieve oldest date in DB and pull historical data from there
    oldest_date_time = None
    start, end = get_times(oldest_date_time)

    for bbox in constants.BBOXES:
        try:
            records = fetch_month_data(start, end, bbox)
            print("\n\nRecords Retrieved:\n\n")
            # print(records, "\n\n\n")
            publish_raw_historical_records(records)
        except:
            print(f"failed at {bbox} for time period {start} - {end}")


if __name__ == "__main__":
    main()
