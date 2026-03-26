"""
Kafka Batch Consumer - Ingest to Landing Zone

Consumes messages from Kafka for a time window and writes to landing zone as JSON.

Pattern: Kafka Topic -> (This Script) -> ./data/landing/{topic}_{timestamp}.json
"""

import json
import time
import os
import uuid
from datetime import datetime
from collections import defaultdict
import boto3
from botocore.exceptions import ClientError
from kafka import KafkaConsumer
from dotenv import load_dotenv
from util import constants

load_dotenv()

dev = os.getenv("DEV")


def ensure_bucket_exists(s3_client, bucket_name: str):
    """Create the bucket if it does not already exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code not in {"404", "NoSuchBucket"}:
            raise
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")


def flush_partitions(s3_client, bucket_name: str, buffered_partitions) -> int:
    """Write buffered records to storage and clear the in-memory partitions."""
    total_records_written = 0

    for date, records in buffered_partitions.items():
        if not records:
            continue

        key = f"landing/airnow/date={date}/{uuid.uuid4()}.json"
        payload = "\n".join(
            json.dumps(record)
            for record in records
        ).encode("utf-8")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=payload,
        )
        total_records_written += len(records)
        if dev == "1" and records:
            print(
                f"Wrote {len(records)} records to {date} partition"
            )

    if dev == "1" and total_records_written:
        print(
            f"Total records written: {total_records_written}"
        )

    buffered_partitions.clear()
    return total_records_written


def consume_data(kafka_topic: str, is_historic: bool = True):
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
    if not kafka_topic:
        raise ValueError("Missing kafka topic parameter")

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
        kafka_topic,
        bootstrap_servers=bootstrap_server,
        auto_offset_reset="earliest",
        group_id="aws_s3_writer_group",
        enable_auto_commit=False,
        key_deserializer=lambda k: k.decode(),
        value_deserializer=lambda v: json.loads(v.decode()),
        max_poll_records=int(
            os.getenv("KAFKA_CONSUMER_MAX_POLL_RECORDS", "2500")
        ),  # batch size
        fetch_max_wait_ms=int(
            os.getenv("KAFKA_CONSUMER_FETCH_MAX_WAIT_MS", "500")
        ),  # wait for batching
        fetch_min_bytes=int(
            os.getenv("KAFKA_CONSUMER_FETCH_MIN_BYTES", "1024")
        ),  # don't fetch tiny payloads
    )

    # -----------------------------
    # Setup S3 client
    # -----------------------------
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_USER"),
        aws_secret_access_key=os.getenv("AWS_PASSWORD"),
        region_name="us-east-1",
    )

    streamflow_bucket = os.getenv("STREAMFLOW_BUCKET", "")

    # Create bucket, if nonexistent
    ensure_bucket_exists(s3_client, streamflow_bucket)

    # -----------------------------
    # Start consuming messages
    # -----------------------------
    last_message_time = time.time()
    if dev == "1":
        print("Starting Kafka consumption...")
    oldest_date_ingested = None
    buffered_partitions = defaultdict(list)
    buffered_record_count = 0
    flush_record_count = int(os.getenv("LANDING_FLUSH_RECORD_COUNT", "25000"))
    max_wait_time = int(os.getenv("MAX_KAFKA_CONSUMER_IDLE_TIME", "30"))

    while True:
        if dev == "1":
            print("Getting records from Kafka topic")
        records = consumer.poll(timeout_ms=1000)

        if not records:
            # Exit if no messages arrive for a while
            if time.time() - last_message_time > max_wait_time:
                if buffered_record_count:
                    flush_partitions(s3_client, streamflow_bucket, buffered_partitions)
                if dev == "1":
                    print("No new messages detected. Exiting consumer.")
                break
            continue

        last_message_time = time.time()

        # Flatten polled messages
        if dev == "1":
            print("Flattening polled messages from Kafka")
        messages = [msg.value for msgs in records.values() for msg in msgs]

        # -----------------------------
        # Buffer date partitions and flush larger batches to storage.
        # -----------------------------
        if dev == "1":
            print("Creating partitions for bucket insert")
        for record in messages:
            record_utc = datetime.strptime(
                record["UTC"], constants.AIRNOW_UTC_DATE_FORMAT
            )
            oldest_date_ingested = (
                record_utc
                if oldest_date_ingested is None
                else min(oldest_date_ingested, record_utc)
            )
            date, _ = record["UTC"].split("T")
            buffered_partitions[date].append(record)

        buffered_record_count += len(messages)

        if buffered_record_count >= flush_record_count:
            flush_partitions(
                s3_client,
                streamflow_bucket,
                buffered_partitions,
            )
            buffered_record_count = 0
            consumer.commit()

    if is_historic and oldest_date_ingested is not None:
        # Update streamflow metadata file with new oldest_date
        body = json.dumps(
            {
                "oldest_loaded_date": oldest_date_ingested.strftime(
                    constants.AIRNOW_UTC_DATE_FORMAT
                ),
                "ingested_at": datetime.now().isoformat(),
            }
        )

        progress_key = os.getenv("STREAMFLOW_BUCKET_PROGRESS_KEY")
        if progress_key:
            if dev == "1":
                print("Creating/Updating streamflow metadata json file in s3 bucket")
            s3_client.put_object(Bucket=streamflow_bucket, Key=progress_key, Body=body)
        else:
            raise ValueError("Missing streamflow bucket progress key value")

    consumer.close()
    if dev == "1":
        print("Kafka ingestion complete.")


if __name__ == "__main__":
    while True:
        choice = input(
            """
            \n\nSelect which consumer to run:
                '1': Historic
                '2': Current\n\n 
            """
        )
        match choice:
            case "1":
                consume_data(os.getenv("RAW_HISTORIC_DATA_KAFKA_TOPIC", ""))
                break
            case "2":
                consume_data(os.getenv("RAW_CURRENT_DATA_KAFKA_TOPIC", ""))
                break
            case _:
                print("Invalid input. Please choose from the options below:")
