"""
Producer for Raw Historical Data Kafka Topic
"""

import os
import json
from datetime import datetime
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
from util import constants

load_dotenv()

yesterday = datetime.now().replace(day=datetime.now().day - 1).strftime("%Y-%m-%dT%H")
now = datetime.now().strftime("%Y-%m-%dT%H")


def fetch_current_month(start=yesterday, end=now):
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
        "BBOX": "-85.13,33.30,-84.20,34.00",  # LongLats for Atlanta, GA Metro Area
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


def main():
    """
    Main function for running producer locally
    """
    records = fetch_current_month()
    print("\n\nRecords Retrieved:\n\n")
    print(records, "\n\n\n")
    publish_raw_historical_records(records)


if __name__ == "__main__":
    main()
