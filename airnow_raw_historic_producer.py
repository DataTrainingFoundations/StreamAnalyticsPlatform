import os
import argparse
import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("AIRNOW_API_KEY", "")
airnow_url = os.getenv("AIRNOW_REALTIME_DATA_URL", "")

if api_key == "":
    raise ValueError("Missing API key")
if airnow_url == "":
    raise ValueError("Missing airnow url")

producer = KafkaProducer(
    bootstrap_servers="localhost:9094", #"kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

yesterday = datetime.now() \
    .replace(day=datetime.now().day - 1) \
    .strftime("%Y-%m-%dT%H")
now = datetime.now().strftime("%Y-%m-%dT%H")

def fetch_current_month(start=yesterday, end=now):
    params = {
        "startDate": start,
        "endDate": end,
        "parameters": "PM25,PM10,OZONE,NO2,CO,SO2",
        "BBOX": "-85.13,33.30,-84.20,34.00", # LongLats for Atlanta, GA Metro Area
        "dataType": "A",
        "format": "application/json",
        "verbose": 1,
        "API_KEY": api_key,
    }
    return requests.get(airnow_url, params=params, timeout=300_000).json()


def publish_records(records):
    for record in records:
        record["source"] = "airnow_historical"
        record["ingested_at"] = datetime.now().isoformat()
        producer.send("airnow_raw_historical", key=record["FullAQSCode"].encode(), value=record)

    producer.flush()
    print("Batch sent.")
    time.sleep(600)  # every 10 minutes


def main():
    records = fetch_current_month()
    print("\n\nRecords Retrieved:\n\n")
    print(records, "\n\n\n")
    publish_records(records)


if __name__ == "__main__":
    main()
