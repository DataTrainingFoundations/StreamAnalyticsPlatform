import json
import os
import time
import pytest
from datetime import datetime
from scripts import ingest_kafka_to_landing
from util import constants


class FakeMessage:
    def __init__(self, value):
        self.value = value


class FakeConsumer:
    def __init__(self, records_sequence):
        self.records_sequence = records_sequence
        self.closed = False

    def poll(self, timeout_ms):
        if self.records_sequence:
            return self.records_sequence.pop(0)
        return {}

    def close(self):
        self.closed = True


class FakeS3Client:
    def __init__(self, existing_buckets=None):
        self.existing_buckets = existing_buckets or []
        self.put_calls = []
        self.created_buckets = []

    def list_buckets(self):
        return {"Buckets": [{"Name": b} for b in self.existing_buckets]}

    def create_bucket(self, Bucket):
        self.created_buckets.append(Bucket)

    def put_object(self, Bucket, Key, Body):
        self.put_calls.append({"Bucket": Bucket, "Key": Key, "Body": Body})


def test_consume_data_valid_two_messages(monkeypatch, tmp_path):
    monkeypatch.setenv("STREAMFLOW_BUCKET", "test-bucket")
    monkeypatch.setenv("DEV", "0")
    monkeypatch.setenv("DOCKER_ENV", "0")
    monkeypatch.setenv("AWS_USER", "dummy")
    monkeypatch.setenv("AWS_PASSWORD", "dummy")

    # use immediate timeout to exit quickly after no-data cycle
    monkeypatch.setattr(constants, "MAX_KAFKA_CONSUMER_IDLE_TIME", 0.000001)

    sample_record = {
        "UTC": datetime.now().strftime(constants.AIRNOW_UTC_DATE_FORMAT),
        "Category": 1,
        "AQI": 10,
        "IntlAQSCode": "123",
        "Parameter": "PM25",
    }

    # first poll returns record, second poll triggers idle exit
    records_sequence = [
        {"topic-1": [FakeMessage(sample_record)]},
        {},
    ]

    fake_consumer = FakeConsumer(records_sequence=records_sequence)
    fake_s3 = FakeS3Client(existing_buckets=[])

    monkeypatch.setattr(ingest_kafka_to_landing, "KafkaConsumer", lambda *args, **kwargs: fake_consumer)
    monkeypatch.setattr(ingest_kafka_to_landing.boto3, "client", lambda *args, **kwargs: fake_s3)

    # set required progress key, so no exception at end
    monkeypatch.setenv("STREAMFLOW_BUCKET_PROGRESS_KEY", "progress.json")

    ingest_kafka_to_landing.consume_data("topic-1", is_historic=True)

    assert fake_consumer.closed is True
    # one object written plus one progress write after finishing
    assert len(fake_s3.put_calls) == 2
    assert fake_s3.created_buckets == ["test-bucket"]


def test_consume_data_raises_on_missing_topic():
    with pytest.raises(ValueError):
        ingest_kafka_to_landing.consume_data("")
