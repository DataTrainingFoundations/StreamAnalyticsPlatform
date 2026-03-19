import pytest

from jobs import spark_session_factory as ssf


class DummySparkContext:
    def __init__(self):
        self.log_level = None

    def setLogLevel(self, level):
        self.log_level = level


class DummySparkSession:
    def __init__(self):
        self.sparkContext = DummySparkContext()
        self.stopped = False

    def stop(self):
        self.stopped = True


class DummyBuilder:
    def __init__(self):
        self.steps = []

    def appName(self, name):
        self.steps.append(("appName", name))
        return self

    def master(self, master):
        self.steps.append(("master", master))
        return self

    def config(self, key, value):
        self.steps.append(("config", key, value))
        return self

    def getOrCreate(self):
        return DummySparkSession()


def test_create_spark_session_with_overrides(monkeypatch):
    dummy_builder = DummyBuilder()
    monkeypatch.setattr(ssf.SparkSession, "builder", dummy_builder)

    session = ssf.create_spark_session(app_name="test-app", master="local[*]", config_overrides={"spark.custom":"value"})

    assert isinstance(session, DummySparkSession)
    assert session.sparkContext.log_level == "WARN"

    # verify critical config chain includes custom override at end
    assert ("appName", "test-app") in dummy_builder.steps
    assert ("master", "local[*]") in dummy_builder.steps
    assert ("config", "spark.custom", "value") in dummy_builder.steps


def test_get_or_create_session_uses_active_session(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(ssf.SparkSession, "getActiveSession", lambda: sentinel)

    result = ssf.get_or_create_session(app_name="ignored")

    assert result is sentinel


def test_get_or_create_session_creates_if_no_active(monkeypatch):
    monkeypatch.setattr(ssf.SparkSession, "getActiveSession", lambda: None)
    dummy_session = DummySparkSession()
    monkeypatch.setattr(ssf, "create_spark_session", lambda app_name=None: dummy_session)

    result = ssf.get_or_create_session(app_name="foo")

    assert result is dummy_session


def test_stop_session_handles_none_and_stops():
    # no-op on None
    ssf.stop_session(None)

    # stops on valid session
    spark = DummySparkSession()
    ssf.stop_session(spark)
    assert spark.stopped is True
