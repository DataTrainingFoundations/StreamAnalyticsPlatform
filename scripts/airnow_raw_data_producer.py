"""
Producer for Raw Historical Data Kafka Topic
"""

import os
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
from util import constants

load_dotenv()

def get_times(oldest_date_time = None):
    """
    datetime(2026, 2, 28) is currently the default
    oldest_date_time is currently None as we haven't passing anything in right now
    the interval is by month so start is start of the month and end is end of the month
    """
    if oldest_date_time is None:
        start = (datetime(2026, 2, 28, 0) - relativedelta(months=1)).strftime("%Y-%m-%dT%H")
        end = (datetime(2026, 2, 28, 23) - timedelta(days=1)).strftime("%Y-%m-%dT%H")
    else:
        start = (oldest_date_time.replace(hour=0) - relativedelta(months=1)).strftime("%Y-%m-%dT%H")
        end = (oldest_date_time.replace(hour=23) - timedelta(days=1)).strftime("%Y-%m-%dT%H")
    return start, end


def fetch_month_data(start, end, bbox):
    """
    Fetches historical data (from yesterday to now by default)
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
        constants.DOCKER_KAFKA_BOOTSTRAP_SERVER
        if docker_env == "1"
        else constants.LOCAL_KAFKA_BOOTSTRAP_SERVER
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for record in records:
        record["source"] = "airnow_historical"
        record["ingested_at"] = datetime.now().isoformat()
        producer.send(
            constants.RAW_HISTORICAL_DATA_KAFKA_TOPIC,
            key=record["FullAQSCode"].encode(),
            value=record,
        )

    producer.flush()
    print("Batch sent.")

def fetch_current_data(bbox):
    """
    Fetches historical data (from the last hour by default)
    """
    api_key = os.getenv("AIRNOW_API_KEY", "")
    airnow_url = os.getenv("AIRNOW_DATA_URL", "")

    if api_key == "":
        raise ValueError("Missing API key")
    if airnow_url == "":
        raise ValueError("Missing airnow url")

    params = {
        "parameters": "PM25,PM10,OZONE,NO2,CO,SO2",
        "BBOX": bbox,  
        "dataType": "A",
        "format": "application/json",
        "verbose": 1,
        "API_KEY": api_key,
    }
    return requests.get(airnow_url, params=params, timeout=300_000).json()


def publish_raw_current_records(records):
    """
    Publishes raw current records to corresponding kafka topic
    """
    docker_env = os.getenv("DOCKER_ENV")
    bootstrap_server = (
        constants.DOCKER_KAFKA_BOOTSTRAP_SERVER
        if docker_env == "1"
        else constants.LOCAL_KAFKA_BOOTSTRAP_SERVER
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for record in records:
        record["source"] = "airnow_current"
        record["ingested_at"] = datetime.now().isoformat()
        producer.send(
            constants.RAW_CURRENT_DATA_KAFKA_TOPIC,
            key=record["FullAQSCode"].encode(),
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
            publish_raw_historical_records(records)
        except:
            print(f"failed at {bbox} for time period {start} - {end}")


if __name__ == "__main__":
    main()
