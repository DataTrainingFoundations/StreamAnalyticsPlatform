import os
import pytest
from datetime import datetime, timedelta

from dags import dag_current_streamflow as current_dag
from dags import dag_historic_streamflow as historic_dag
from util import constants


def test_produce_current_data_with_dev_1_limited_to_five(monkeypatch):
    # Setup small set of boxes but ensure >5 processing is capped in DEV=1
    monkeypatch.setattr(current_dag, "DEV", "1")
    monkeypatch.setattr(constants, "BBOXES", [f"bbox{i}" for i in range(10)])

    fetched = []
    published = []

    def fake_fetch_current_data(bbox):
        fetched.append(bbox)
        return [f"record-{bbox}"]

    def fake_publish(records, topic):
        published.append((records, topic))

    monkeypatch.setattr(current_dag, "fetch_current_data", fake_fetch_current_data)
    monkeypatch.setattr(current_dag, "publish_raw_historical_records", fake_publish)
    monkeypatch.setenv("RAW_CURRENT_DATA_KAFKA_TOPIC", "topic1")

    current_dag.produce_current_data(ti={"ti": None})

    assert len(fetched) == 5
    assert len(published) == 5
    for r, topic in published:
        assert topic == "topic1"
        assert r


def test_produce_current_data_propagates_exception(monkeypatch):
    monkeypatch.setattr(current_dag, "DEV", "")
    monkeypatch.setattr(constants, "BBOXES", ["bbox1"])

    def fake_fetch_current_data(_):
        raise Exception("boom")

    monkeypatch.setattr(current_dag, "fetch_current_data", fake_fetch_current_data)
    monkeypatch.setattr(current_dag, "publish_raw_historical_records", lambda *_: None)

    with pytest.raises(Exception, match="boom"):
        current_dag.produce_current_data(ti={"ti": None})


def test_archive_raw_current_data_requires_env(monkeypatch):
    monkeypatch.delenv("STREAMFLOW_BUCKET_ARCHIVE_PREFIX", raising=False)
    monkeypatch.delenv("STREAMFLOW_BUCKET_LANDING_PREFIX", raising=False)
    with pytest.raises(ValueError):
        current_dag.archive_raw_current_data()


def test_archive_raw_current_data_calls_move(monkeypatch):
    monkeypatch.setenv("STREAMFLOW_BUCKET_ARCHIVE_PREFIX", "archive")
    monkeypatch.setenv("STREAMFLOW_BUCKET_LANDING_PREFIX", "landing")
    called = {}

    def fake_move_processed_data(src, dst):
        called["src"] = src
        called["dst"] = dst

    monkeypatch.setattr(current_dag, "move_processed_data", fake_move_processed_data)

    current_dag.archive_raw_current_data()

    assert called["src"] == "landing"
    assert called["dst"] == "archive"


def test_decide_ingestion_stops_when_old_date(monkeypatch):
    oldest = datetime.strptime("2019-01-01T00:00", constants.AIRNOW_UTC_DATE_FORMAT)

    monkeypatch.setattr(historic_dag, "get_oldest_record_date", lambda: oldest)
    class DummyTI:
        def __init__(self):
            self.value = None
        def xcom_push(self, key, value):
            self.value = (key, value)

    ti = DummyTI()
    decision = historic_dag.decide_ingestion(ti=ti)

    assert decision == "stop_pipeline"
    assert ti.value == ("oldest_date", oldest)


def test_decide_ingestion_produces_data_when_recent(monkeypatch):
    oldest = datetime.strptime("2027-01-01T00:00", constants.AIRNOW_UTC_DATE_FORMAT)
    monkeypatch.setattr(historic_dag, "get_oldest_record_date", lambda: oldest)

    class DummyTI:
        def xcom_push(self, key, value):
            pass

    decision = historic_dag.decide_ingestion(ti=DummyTI())
    assert decision == "produce_raw_data_to_kafka"


def test_produce_historical_data_uses_xcom_and_publishes(monkeypatch):
    monkeypatch.setattr(constants, "BBOXES", ["bbox1"])
    monkeypatch.setattr(historic_dag, "get_times", lambda oldest: ("start", "end"))

    published = []
    def fake_fetch_historic_data(start, end, bbox):
        assert (start, end) == ("start", "end")
        return [f"rec-{bbox}"]

    def fake_publish(records, topic):
        published.append((records, topic))

    monkeypatch.setattr(historic_dag, "fetch_historic_data", fake_fetch_historic_data)
    monkeypatch.setattr(historic_dag, "publish_raw_historical_records", fake_publish)

    class DummyTI:
        def xcom_pull(self, task_ids, key):
            assert task_ids == "branch_decision"
            assert key == "oldest_date"
            return datetime.strptime("2027-01-01T00:00", constants.AIRNOW_UTC_DATE_FORMAT)

    historic_dag.produce_historical_data(ti=DummyTI())

    assert len(published) == 1


def test_pause_this_dag_sets_paused(monkeypatch):
    class DummyQuery:
        def __init__(self):
            self.is_paused = False

        def filter(self, _):
            return self

        def first(self):
            return self

    query = DummyQuery()

    class DummySession:
        def query(self, cls):
            assert cls == historic_dag.DagModel
            return query

    historic_dag.pause_this_dag("streamflow_historic", session=DummySession())

    assert query.is_paused is True


def test_archive_raw_historic_data_calls_move(monkeypatch):
    monkeypatch.setenv("STREAMFLOW_BUCKET_ARCHIVE_PREFIX", "archive")
    monkeypatch.setenv("STREAMFLOW_BUCKET_LANDING_PREFIX", "landing")
    called = {}

    def fake_move_processed_data(src, dst):
        called["src"] = src
        called["dst"] = dst

    monkeypatch.setattr(historic_dag, "move_processed_data", fake_move_processed_data)
    historic_dag.archive_raw_historic_data()

    assert called["src"] == "landing"
    assert called["dst"] == "archive"
