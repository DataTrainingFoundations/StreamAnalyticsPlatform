"""
Kafka Batch Consumer - Ingest to Landing Zone

Consumes messages from Kafka for a time window and writes to landing zone as JSON.

Pattern: Kafka Topic -> (This Script) -> ./data/landing/{topic}_{timestamp}.json
"""

import json
import time
import os
import uuid
import boto3
from kafka import KafkaConsumer
from dotenv import load_dotenv
from util import constants

load_dotenv()


def consume_historical_data():
    """
    Consume historical data from Kafka and write to landing zone (MinIO).
    """

    # Create Kafka Consumer
    docker_env = os.getenv("DOCKER_ENV")
    bootstrap_server = (
        os.getenv("DOCKER_KAFKA_BOOTSTRAP_SERVER")
        if docker_env == "1"
        else os.getenv("LOCAL_KAFKA_BOOTSTRAP_SERVER")
    )
    consumer = KafkaConsumer(
        os.getenv("RAW_HISTORICAL_DATA_KAFKA_TOPIC"),
        bootstrap_servers=bootstrap_server,
        auto_offset_reset="earliest",
        group_id="minio_writer_group",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode())
    )

    # Create MinIO client and create bucket, if nonexistent
    endpoint = (
        os.getenv("DOCKER_MINIO_ENDPOINT")
        if docker_env == "1"
        else os.getenv("LOCAL_MINIO_ENDPOINT")
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )

    existing_buckets = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    historical_data_bucket = os.getenv("MINIO_HISTORICAL_DATA_BUCKET")
    if historical_data_bucket and historical_data_bucket not in existing_buckets:
        s3_client.create_bucket(Bucket=historical_data_bucket)
        print(f"Created bucket: {historical_data_bucket}")

    # Consume and batch messages and insert into bucket
    batch = []
    last_message_time = time.time()

    print("Starting Kafka batch consumption...")

    while True:

        records = consumer.poll(timeout_ms=1000)

        if not records:
            # Exit if no messages arrive for a while
            if time.time() - last_message_time > constants.MAX_KAFKA_CONSUMER_IDLE_TIME:
                print("No new messages detected. Exiting consumer.")
                break
            continue

        for _, messages in records.items():
            for message in messages:
                batch.append(message.value)

        last_message_time = time.time()

        if len(batch) >= constants.KAFKA_BATCH_SIZE:

            key = f"{uuid.uuid4()}.json"

            s3_client.put_object(
                Bucket=historical_data_bucket,
                Key=key,
                Body=json.dumps(batch).encode(),
            )

            print(f"Wrote {len(batch)} messages to MinIO as {key}")

            batch = []

    # Write remaining messages
    if batch:
        key = f"{uuid.uuid4()}.json"

        s3_client.put_object(
            Bucket=historical_data_bucket,
            Key=key,
            Body=json.dumps(batch).encode(),
        )

        print(f"Wrote final {len(batch)} messages to MinIO")

    consumer.close()

    print("Kafka batch ingestion complete.")

if __name__ == "__main__":
    consume_historical_data()
