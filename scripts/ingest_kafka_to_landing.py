"""
Kafka Batch Consumer - Ingest to Landing Zone

Consumes messages from Kafka for a time window and writes to landing zone as JSON.

Pattern: Kafka Topic -> (This Script) -> ./data/landing/{topic}_{timestamp}.json
"""

import json
import time
import os
import uuid
from collections import defaultdict
import boto3
from kafka import KafkaConsumer
from dotenv import load_dotenv
from util import constants

load_dotenv()

def consume_historical_data():
    """
    Consume historical data from Kafka and write to MinIO with date/hour partitioning.

    This function sets up a Kafka consumer to poll messages from the raw historical data topic,
    groups the records by date and hour, and writes them to MinIO (S3-compatible storage)
    in the landing zone with appropriate partitioning for efficient querying.

    The function will exit if no messages are received for a configurable idle time.

    Environment Variables Required:
        - DOCKER_ENV: Determines whether to use Docker or local environment settings
        - DOCKER_KAFKA_BOOTSTRAP_SERVER / LOCAL_KAFKA_BOOTSTRAP_SERVER: Kafka bootstrap servers
        - RAW_HISTORIC_DATA_KAFKA_TOPIC: Kafka topic to consume from
        - AWS_ENDPOINT: Amazon S3 bucket endpoint URL
        - AWS_USER: AWS access key
        - AWS_PASSWORD: AWS secret key
        - STREAMFLOW_BUCKET: S3 bucket name

    Raises:
        Exception: If Kafka consumer setup or MinIO operations fail.
    """
    docker_env = os.getenv("DOCKER_ENV")

    # -----------------------------
    # Setup Kafka consumer
    # -----------------------------
    bootstrap_server = (
        os.getenv("DOCKER_KAFKA_BOOTSTRAP_SERVER")
        if docker_env == "1"
        else os.getenv("LOCAL_KAFKA_BOOTSTRAP_SERVER")
    )

    consumer = KafkaConsumer(
        os.getenv("RAW_HISTORIC_DATA_KAFKA_TOPIC"),
        bootstrap_servers=bootstrap_server,
        auto_offset_reset="earliest",
        group_id="minio_writer_group",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode()),
    )

    # -----------------------------
    # Setup MinIO client
    # -----------------------------
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_USER"),
        aws_secret_access_key=os.getenv("AWS_PASSWORD"),
        region_name="us-east-1"
    )

    streamflow_bucket = os.getenv("STREAMFLOW_BUCKET")

    # Create bucket, if nonexistent
    existing_buckets = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    if streamflow_bucket not in existing_buckets:
        s3_client.create_bucket(Bucket=streamflow_bucket)
        print(f"Created bucket: {streamflow_bucket}")

    # -----------------------------
    # Start consuming messages
    # -----------------------------
    last_message_time = time.time()
    print("Starting Kafka consumption...")

    while True:
        records = consumer.poll(timeout_ms=1000)

        if not records:
            # Exit if no messages arrive for a while
            if time.time() - last_message_time > constants.MAX_KAFKA_CONSUMER_IDLE_TIME:
                print("No new messages detected. Exiting consumer.")
                break
            continue

        last_message_time = time.time()

        # Flatten polled messages
        messages = [msg.value for msgs in records.values() for msg in msgs]

        # -----------------------------
        # Group by date/hour partitions
        # -----------------------------
        date_partitions = defaultdict(lambda: defaultdict(list))
        for record in messages:
            date, hour = record["UTC"].split("T")
            hour = hour[:2]  # Keep only hour and drop minutes
            date_partitions[date][hour].append(record)

        # -----------------------------
        # Write files per date/hour
        # -----------------------------
        for date, hour_partitions in date_partitions.items():
            date_partition = f"landing/airnow/date={date}"
            for hour, records in hour_partitions.items():
                key = f"landing/airnow/date={date}/hour={hour}/{uuid.uuid4()}.json"
                s3_client.put_object(
                    Bucket=streamflow_bucket,
                    Key=key,
                    Body="\n".join(json.dumps(r) for r in records).encode(),
                )
            print(f"Wrote {len(hour_partitions)} records to {date_partition}")

    consumer.close()
    print("Kafka ingestion complete.")

if __name__ == "__main__":
    consume_historical_data()
