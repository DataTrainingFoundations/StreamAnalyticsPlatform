import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_current():
    params = {
        "format": "application/json",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "distance": 50,
        "API_KEY": os.getenv("AIRNOW_API_KEY")
    }
    return requests.get(BASE_URL, params=params).json()

def run_stream():
    while True:
        records = fetch_current()

        for record in records:
            record["ingested_at"] = datetime.now().isoformat()
            producer.send("airnow_raw_current", record)

        producer.flush()
        print("Batch sent.")
        time.sleep(600)  # every 10 minutes
