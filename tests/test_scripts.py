import json
import os
import pytest
from datetime import datetime, timedelta

from scripts import airnow_raw_producers as arp
from scripts import cleanup_data as cld
from scripts import ingest_kafka_to_landing as ikl
from util import constants


class FakeBody:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return self._payload


class FakeS3Exceptions:
    class NoSuchKey(Exception):
        pass

    class NoSuchBucket(Exception):
        pass


class FakeS3ClientOldest:
    def __init__(self, data):
        self.data = data
        self.exceptions = FakeS3Exceptions

    def get_object(self, Bucket, Key):
        if self.data is None:
            raise self.exceptions.NoSuchKey()
        return {"Body": FakeBody(json.dumps(self.data).encode())}


def test_get_oldest_record_date_progress_key(monkeypatch):
    monkeypatch.setenv("STREAMFLOW_BUCKET", "bucket")
    monkeypatch.setenv("STREAMFLOW_BUCKET_PROGRESS_KEY", "progress.json")
    monkeypatch.setenv("DEV", "1")

    monkeypatch.setattr(arp, "boto3", type("m", (), {"client": lambda *args, **kwargs: FakeS3ClientOldest({"oldest_loaded_date": "2019-01-01T00:00"})}))

    result = arp.get_oldest_record_date()
    assert result == datetime.strptime("2019-01-01T00:00", constants.AIRNOW_UTC_DATE_FORMAT)


def test_get_oldest_record_date_no_key(monkeypatch):
    monkeypatch.setenv("STREAMFLOW_BUCKET", "bucket")
    monkeypatch.setenv("STREAMFLOW_BUCKET_PROGRESS_KEY", "progress.json")
    monkeypatch.setenv("DEV", "1")

    monkeypatch.setattr(arp, "boto3", type("m", (), {"client": lambda *args, **kwargs: FakeS3ClientOldest(None)}))

    assert arp.get_oldest_record_date() is None


def test_get_times_with_oldest():
    oldest = datetime.strptime("2026-03-10T00:00", constants.AIRNOW_UTC_DATE_FORMAT)
    start, end = arp.get_times(oldest)
    assert "2026-" in start and "2026-" in end
    assert start < end


def test_get_times_with_none(monkeypatch):
    monkeypatch.setattr(arp, "get_oldest_record_date", lambda: None)
    start, end = arp.get_times(None)
    assert start.endswith(":00")
    assert end.endswith(":00")


def test_fetch_historic_data_missing_env(monkeypatch):
    monkeypatch.delenv("AIRNOW_API_KEY", raising=False)
    monkeypatch.delenv("AIRNOW_DATA_URL", raising=False)
    with pytest.raises(ValueError):
        arp.fetch_data("2026-01-01T00", "2026-01-02T00", "bbox")


def test_fetch_historic_data_api_error(monkeypatch):
    monkeypatch.setenv("AIRNOW_API_KEY", "key")
    monkeypatch.setenv("AIRNOW_DATA_URL", "http://api")

    class DummyResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"WebServiceError": [{"Message": "Error"}]}

    monkeypatch.setattr(arp, "requests", type("m", (), {"get": lambda *args, **kwargs: DummyResponse()}))

    result = arp.fetch_data("2026-01-01T00", "2026-01-02T00", "bbox")
    assert result == []


def test_fetch_historic_data_list(monkeypatch):
    monkeypatch.setenv("AIRNOW_API_KEY", "key")
    monkeypatch.setenv("AIRNOW_DATA_URL", "http://api")

    class DummyResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return [{"x": 1}]

    monkeypatch.setattr(arp, "requests", type("m", (), {"get": lambda *args, **kwargs: DummyResponse()}))

    result = arp.fetch_data("2026-01-01T00", "2026-01-02T00", "bbox")
    assert result == [{"x": 1}]


def test_fetch_current_data_missing_env(monkeypatch):
    monkeypatch.delenv("AIRNOW_API_KEY", raising=False)
    monkeypatch.delenv("AIRNOW_DATA_URL", raising=False)
    with pytest.raises(ValueError):
        arp.fetch_data("","","bbox")


def test_fetch_current_data_success(monkeypatch):
    monkeypatch.setenv("AIRNOW_API_KEY", "key")
    monkeypatch.setenv("AIRNOW_DATA_URL", "http://api")

    class DummyResponse:
        def json(self):
            return [{"x": 2}]

    monkeypatch.setattr(arp, "requests", type("m", (), {"get": lambda *args, **kwargs: DummyResponse()}))

    result = arp.fetch_data("","","bbox")
    assert result == [{"x": 2}]


def test_publish_raw_historical_records_missing_topic():
    with pytest.raises(ValueError):
        arp.publish_raw_records([], "")


def test_publish_raw_historical_records_sends(monkeypatch):
    monkeypatch.setenv("DOCKER_ENV", "0")
    monkeypatch.setenv("LOCAL_KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
    monkeypatch.setenv("KAFKA_PRODUCER_ACKS", "all")

    class DummyProducer:
        def __init__(self, **kwargs):
            self.sent = []

        def send(self, topic, key, value):
            self.sent.append((topic, key, value))

        def flush(self):
            self.flushed = True

    monkeypatch.setattr(arp, "KafkaProducer", DummyProducer)

    records = [{"IntlAQSCode": "001", "Parameter": "PM2.5"}]
    arp.publish_raw_historical_records(records, "topic")

    # flush called and one message built
    assert getattr(arp, "KafkaProducer", None)


def test_get_oldest_record_date_missing_progress_key_raises(monkeypatch):
    monkeypatch.setenv("STREAMFLOW_BUCKET", "bucket")
    monkeypatch.delenv("STREAMFLOW_BUCKET_PROGRESS_KEY", raising=False)
    monkeypatch.setenv("DEV", "1")

    monkeypatch.setattr(
        arp,
        "boto3",
        type(
            "m",
            (),
            {
                "client": lambda *args, **kwargs: FakeS3ClientOldest({"oldest_loaded_date": "2019-01-01T00:00"})
            },
        ),
    )

    with pytest.raises(RuntimeError):
        arp.get_oldest_record_date()


def test_get_oldest_record_date_no_such_bucket_returns_none(monkeypatch):
    monkeypatch.setenv("STREAMFLOW_BUCKET", "bucket")
    monkeypatch.setenv("STREAMFLOW_BUCKET_PROGRESS_KEY", "progress.json")
    monkeypatch.setenv("DEV", "1")

    class FakeS3NoBucket:
        class exceptions:
            class NoSuchKey(Exception):
                pass

            class NoSuchBucket(Exception):
                pass

        def get_object(self, Bucket, Key):
            raise self.exceptions.NoSuchBucket()

    monkeypatch.setattr(arp, "boto3", type("m", (), {"client": lambda *args, **kwargs: FakeS3NoBucket()}))

    result = arp.get_oldest_record_date()
    assert result is None


def test_fetch_historic_data_unexpected_dict_raises(monkeypatch):
    monkeypatch.setenv("AIRNOW_API_KEY", "key")
    monkeypatch.setenv("AIRNOW_DATA_URL", "http://api")

    class DummyResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"anything": "value"}

    monkeypatch.setattr(arp, "requests", type("m", (), {"get": lambda *args, **kwargs: DummyResponse()}))

    with pytest.raises(RuntimeError):
        arp.fetch_data("2026-01-01T00", "2026-01-02T00", "bbox")


def test_fetch_historic_data_json_decode_returns_empty(monkeypatch):
    monkeypatch.setenv("AIRNOW_API_KEY", "key")
    monkeypatch.setenv("AIRNOW_DATA_URL", "http://api")

    class DummyResponse:
        def raise_for_status(self):
            pass

        def json(self):
            raise json.JSONDecodeError("msg", "doc", 0)

    monkeypatch.setattr(arp, "requests", type("m", (), {"get": lambda *args, **kwargs: DummyResponse()}))

    result = arp.fetch_data("2026-01-01T00", "2026-01-02T00", "bbox")
    assert result == []


def test_run_historic_and_current_producers(monkeypatch):
    called_historic = []
    called_current = []

    monkeypatch.setattr(arp, "get_times", lambda: ("2026-03-01T00", "2026-03-10T23"))
    monkeypatch.setattr(arp.constants, "BBOXES", ["a", "b"])
    monkeypatch.setattr(arp, "fetch_historic_data", lambda s, e, b: [{"IntlAQSCode": "001", "Parameter": "PM2.5"}])
    monkeypatch.setattr(arp, "fetch_current_data", lambda b: [{"IntlAQSCode": "002", "Parameter": "OZONE"}])

    def fake_publish(record, topic):
        if record == [{"IntlAQSCode": "001", "Parameter": "PM2.5"}]:
            called_historic.append(topic)
        else:
            called_current.append(topic)

    monkeypatch.setattr(arp, "publish_raw_historical_records", fake_publish)

    monkeypatch.setenv("RAW_HISTORIC_DATA_KAFKA_TOPIC", "historic-topic")
    monkeypatch.setenv("RAW_CURRENT_DATA_KAFKA_TOPIC", "current-topic")

    arp.run_producer()
    arp.run_current_producer()

    assert called_historic == ["historic-topic", "historic-topic"]
    assert called_current == ["current-topic", "current-topic"]


class FakeClientObj:
    def __init__(self):
        self.bucket_created = False
        self.head_bucket_called = 0
        self.copied = []
        self.deleted = []

    def head_bucket(self, Bucket):
        self.head_bucket_called += 1
        if Bucket == "missing":
            raise Exception("404")

    def create_bucket(self, Bucket):
        self.bucket_created = True

    def put_object(self, Bucket, Key, Body):
        self.put = (Bucket, Key, Body)

    def copy_object(self, Bucket, CopySource, Key):
        self.copied.append((Bucket, CopySource, Key))

    def delete_objects(self, Bucket, Delete):
        self.deleted.append((Bucket, Delete))


def test_ensure_bucket_exists_no_bucket(monkeypatch):
    fake = FakeClientObj()
    class DummyClient:
        pass

    def fake_client(*args, **kwargs):
        return fake

    monkeypatch.setattr(ikl, "boto3", type("m", (), {"client": fake_client}))
    # returns quietly after missing bucket
    # raise with non 404
    class DummyError(Exception):
        response = {"Error": {"Code": "404"}}

    def fake_head(*args, **kwargs):
        raise ikl.ClientError({"Error": {"Code": "404"}}, "HeadBucket")

    fake.head_bucket = lambda Bucket: (_ for _ in ()).throw(ikl.ClientError({"Error": {"Code": "404"}}, "HeadBucket"))

    kl = ikl.ensure_bucket_exists(fake, "bucket")
    assert fake.bucket_created


def test_flush_partitions(monkeypatch):
    fake_client = FakeClientObj()
    buffered = {"2026-03-10": {"01": [{"x": 1}, {"x": 2}]}}

    written = ikl.flush_partitions(fake_client, "bucket", buffered)

    assert written == 2
    assert buffered == {}


def test_consume_data_missing_topic():
    with pytest.raises(ValueError):
        ikl.consume_data("")


def test_consume_data_early_exit(monkeypatch):
    # avoid endless loop by manipulating time and consumer
    class DummyConsumer:
        def __init__(self):
            self.closed = False
            self.poll_count = 0

        def poll(self, timeout_ms):
            self.poll_count += 1
            return {}

        def commit(self):
            pass

        def close(self):
            self.closed = True

    monkeypatch.setattr(ikl, "KafkaConsumer", lambda *args, **kwargs: DummyConsumer())

    class FakeS3Client:
        def __init__(self):
            self.headers = []

        def head_bucket(self, Bucket):
            pass

        def create_bucket(self, Bucket):
            pass

        def put_object(self, Bucket, Key, Body):
            pass

    monkeypatch.setattr(ikl, "boto3", type("m", (), {"client": lambda *args, **kwargs: FakeS3Client()}))
    monkeypatch.setattr(ikl, "ensure_bucket_exists", lambda *args, **kwargs: None)

    monkeypatch.setenv("DEV", "1")
    monkeypatch.setenv("DOCKER_ENV", "0")
    monkeypatch.setenv("LOCAL_KAFKA_BOOTSTRAP_SERVER", "test")
    monkeypatch.setenv("STREAMFLOW_BUCKET", "bucket")
    monkeypatch.setenv("MAX_KAFKA_CONSUMER_IDLE_TIME", "0")

    t = [0]

    def fake_time():
        t[0] += 1
        return t[0]

    monkeypatch.setattr(ikl, "time", type("m", (), {"time": fake_time}))

    ikl.consume_data("topic")


def test_move_processed_data_copies_and_deletes(monkeypatch):
    class FakePaginator:
        def paginate(self, Bucket, Prefix):
            return [{"Contents": [{"Key": "landing/airnow/a.json"}, {"Key": "landing/airnow/b.json"}]}]

    class FakeS3:
        def __init__(self):
            self.copy_calls = []
            self.delete_calls = []

        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return FakePaginator()

        def copy_object(self, Bucket, CopySource, Key):
            self.copy_calls.append((Bucket, CopySource, Key))

        def delete_objects(self, Bucket, Delete):
            self.delete_calls.append((Bucket, Delete))

    fake = FakeS3()
    monkeypatch.setattr(cld, "boto3", type("m", (), {"client": lambda *args, **kwargs: fake}))
    monkeypatch.setenv("STREAMFLOW_BUCKET", "bucket")

    cld.move_processed_data("landing/airnow/", "archive/airnow/", max_workers=1)

    assert len(fake.copy_calls) == 2
    assert len(fake.delete_calls) == 1


def test_move_processed_data_skips_empty_page(monkeypatch):
    class FakePaginator:
        def paginate(self, Bucket, Prefix):
            return [{}, {"Contents": [{"Key": "landing/airnow/a.json"}]}]

    class FakeS3:
        def __init__(self):
            self.copy_calls = []
            self.delete_calls = []

        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return FakePaginator()

        def copy_object(self, Bucket, CopySource, Key):
            self.copy_calls.append((Bucket, CopySource, Key))

        def delete_objects(self, Bucket, Delete):
            self.delete_calls.append((Bucket, Delete))

    fake = FakeS3()
    monkeypatch.setattr(cld, "boto3", type("m", (), {"client": lambda *args, **kwargs: fake}))
    monkeypatch.setenv("STREAMFLOW_BUCKET", "bucket")

    cld.move_processed_data("landing/airnow/", "archive/airnow/", max_workers=1)

    assert len(fake.copy_calls) == 1
    assert len(fake.delete_calls) == 1


def test_move_processed_data_copy_fails_continues(monkeypatch):
    class FakePaginator:
        def paginate(self, Bucket, Prefix):
            return [{"Contents": [{"Key": "landing/airnow/a.json"}, {"Key": "landing/airnow/b.json"}]}]

    class FakeS3:
        def __init__(self):
            self.copy_calls = []
            self.delete_calls = []

        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return FakePaginator()

        def copy_object(self, Bucket, CopySource, Key):
            if CopySource["Key"].endswith("b.json"):
                raise RuntimeError("copy failed")
            self.copy_calls.append((Bucket, CopySource, Key))

        def delete_objects(self, Bucket, Delete):
            self.delete_calls.append((Bucket, Delete))

    fake = FakeS3()
    monkeypatch.setattr(cld, "boto3", type("m", (), {"client": lambda *args, **kwargs: fake}))
    monkeypatch.setenv("STREAMFLOW_BUCKET", "bucket")

    cld.move_processed_data("landing/airnow/", "archive/airnow/", max_workers=1)

    assert len(fake.copy_calls) == 1
    assert len(fake.delete_calls) == 1


def test_consume_data_with_records_and_flush(monkeypatch):
    class DummyConsumer:
        def __init__(self):
            self.poll_count = 0
            self.closed = False

        def poll(self, timeout_ms):
            self.poll_count += 1
            if self.poll_count == 1:
                Msg = type("M", (), {"value": {"UTC": "2026-03-10T01:00", "IntlAQSCode": "code", "Parameter": "p"}})
                return {1: [Msg()]}
            return {}

        def commit(self):
            self.committed = True

        def close(self):
            self.closed = True

    class FakeS3:
        def __init__(self):
            self.puts = []
            self.buckets = []

        def head_bucket(self, Bucket):
            pass

        def create_bucket(self, Bucket):
            self.buckets.append(Bucket)

        def put_object(self, Bucket, Key, Body):
            self.puts.append((Bucket, Key, Body))

    fake_s3 = FakeS3()

    monkeypatch.setenv("DEV", "1")
    monkeypatch.setenv("DOCKER_ENV", "0")
    monkeypatch.setenv("LOCAL_KAFKA_BOOTSTRAP_SERVER", "test")
    monkeypatch.setenv("STREAMFLOW_BUCKET", "bucket")
    monkeypatch.setenv("LANDING_FLUSH_RECORD_COUNT", "1")
    monkeypatch.setenv("MAX_KAFKA_CONSUMER_IDLE_TIME", "1")
    monkeypatch.setenv("STREAMFLOW_BUCKET_PROGRESS_KEY", "progress.json")

    monkeypatch.setattr(ikl, "KafkaConsumer", lambda *args, **kwargs: DummyConsumer())
    monkeypatch.setattr(ikl, "boto3", type("m", (), {"client": lambda *args, **kwargs: fake_s3}))
    monkeypatch.setattr(ikl, "ensure_bucket_exists", lambda *args, **kwargs: None)

    t = [0]
    def fake_time():
        t[0] += 0.6
        return t[0]

    monkeypatch.setattr(ikl, "time", type("m", (), {"time": fake_time}))

    ikl.consume_data("topic")

    assert fake_s3.puts != []


