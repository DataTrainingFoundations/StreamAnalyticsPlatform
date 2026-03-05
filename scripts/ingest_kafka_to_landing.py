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


def consume_historical_data_batch():
    """
    Consume historical data from Kafka and write to landing zone (MinIO).
    """

    # Create Kafka Consumer
    docker_env = os.getenv("DOCKER_ENV")
    bootstrap_server = (
        constants.DOCKER_KAFKA_BOOTSTRAP_SERVER
        if docker_env == "1"
        else constants.LOCAL_KAFKA_BOOTSTRAP_SERVER
    )
    consumer = KafkaConsumer(
        constants.RAW_HISTORICAL_DATA_KAFKA_TOPIC,
        bootstrap_servers=bootstrap_server,
        auto_offset_reset="earliest",
        group_id="minio_writer_group",
        enable_auto_commit=True,
    )

    # Create MinIO client and create bucket, if nonexistent
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    endpoint = (
        constants.DOCKER_MINIO_ENDPOINT
        if docker_env == "1"
        else constants.LOCAL_MINIO_ENDPOINT
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
    )

    existing_buckets = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    if constants.MINIO_HISTORICAL_DATA_BUCKET not in existing_buckets:
        s3_client.create_bucket(Bucket=constants.MINIO_HISTORICAL_DATA_BUCKET)
        print(f"Created buckedt: {constants.MINIO_HISTORICAL_DATA_BUCKET}")

    # Consume and batch messages and insert into bucket
    batch = []
    last_batch_time = time.time()

    for message in consumer:
        # Convert message bytes to string
        batch.append(message.value.decode())

        # Check if batch should be written
        if (
            len(batch) >= constants.KAFKA_BATCH_SIZE
            or (time.time() - last_batch_time) >= constants.KAFKA_BATCH_INTERVAL
        ):
            if batch:
                # Generate unique filename for batch
                key = f"{uuid.uuid4()}.json"

                # Write batch as JSON array to MinIO
                s3_client.put_object(
                    Bucket=constants.MINIO_HISTORICAL_DATA_BUCKET,
                    Key=key,
                    Body=json.dumps(batch).encode(),
                )
                print(f"Wrote batch of {len(batch)} messages to MinIO as {key}")

                # Clear batch and reset timer
                batch = []
                last_batch_time = time.time()


if __name__ == "__main__":
    consume_historical_data_batch()
