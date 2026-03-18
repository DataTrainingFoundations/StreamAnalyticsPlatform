import re
import pytest

from jobs import etl_job


class FakeWriter:
    def __init__(self, shared):
        self.shared = shared
        self.mode_value = None
        self.partition_cols = None

    def mode(self, mode):
        self.mode_value = mode
        return self

    def partitionBy(self, *cols):
        self.partition_cols = cols
        return self

    def parquet(self, path):
        self.shared.setdefault("write_paths", []).append(
            {
                "path": path,
                "mode": self.mode_value,
                "partitionBy": self.partition_cols,
            }
        )
        return None


class FakeDataFrame:
    def __init__(self, shared):
        self.shared = shared

    def filter(self, *_args, **_kwargs):
        self.shared["filter_called"] = True
        return self

    def withColumn(self, name, *_args, **_kwargs):
        self.shared.setdefault("with_columns", []).append(name)
        return self

    def drop(self, col_name):
        self.shared.setdefault("drop_columns", []).append(col_name)
        return self

    def drop_duplicates(self, columns):
        self.shared["drop_duplicates"] = columns
        return self

    def toDF(self, *cols):
        self.shared["toDF_columns"] = cols
        return self

    def select(self, *cols):
        self.shared.setdefault("select_calls", []).append(cols)
        return self

    def distinct(self):
        self.shared["distinct_called"] = True
        return self

    @property
    def write(self):
        return FakeWriter(self.shared)

    def __getattr__(self, name):
        if name == "columns":
            return ["Date", "Hour", "IntlAQSCode", "Parameter"]

        class AnyAttr:
            def __eq__(self, other):
                return True

            def __ne__(self, other):
                return True

            def cast(self, _type):
                return self

        return AnyAttr()


class FakeSpark:
    def __init__(self, shared):
        self.shared = shared
        self.read = self

    def json(self, path):
        self.shared["raw_json_path"] = path
        return FakeDataFrame(self.shared)

    def parquet(self, path):
        self.shared["parquet_read_path"] = path
        return FakeDataFrame(self.shared)


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, Bucket, Prefix, Delimiter):
        return self.pages


class FakeS3Client:
    def __init__(self, pages):
        self.pages = pages

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return FakePaginator(self.pages)


class FakeColumn:
    def __init__(self, expr):
        self.expr = expr

    def cast(self, _type):
        return self

    def __eq__(self, other):
        return True

    def __ne__(self, other):
        return True


class FakeFunctions:
    @staticmethod
    def col(_name):
        return FakeColumn(f"col({_name})")

    @staticmethod
    def concat_ws(_sep, *args):
        return FakeColumn(f"concat_ws({_sep},{args})")

    @staticmethod
    def lit(value):
        return FakeColumn(f"lit({value})")

    @staticmethod
    def md5(_col):
        return FakeColumn("md5")

    @staticmethod
    def to_date(_col, _fmt):
        return FakeColumn("to_date")

    @staticmethod
    def year(_date):
        return 2000

    @staticmethod
    def month(_date):
        return 1

    @staticmethod
    def dayofmonth(_date):
        return 1

    @staticmethod
    def dayofweek(_date):
        return 1

    @staticmethod
    def when(cond, result=None):
        class WhenObj:
            def __init__(self, cond, result):
                self.conds = [(cond, result)]

            def when(self, cond, result=None):
                self.conds.append((cond, result))
                return self

            def otherwise(self, default=None):
                return default

        return WhenObj(cond, result)


def test_get_partition_dates_parses_date_prefixes(monkeypatch):
    monkeypatch.setattr(etl_job, "dev", "0")
    monkeypatch.setattr(etl_job, "streamflow_bucket", "test-bucket")

    pages = [
        {
            "CommonPrefixes": [
                {"Prefix": "bronze/airnow/date=2026-03-10/"},
                {"Prefix": "bronze/airnow/date=2026-03-11/"},
                {"Prefix": "bronze/airnow/date=not-a-date/"},
            ]
        }
    ]

    def fake_boto_client(service_name, **_kwargs):
        assert service_name == "s3"
        return FakeS3Client(pages)

    monkeypatch.setattr(etl_job.boto3, "client", fake_boto_client)

    result = etl_job.get_partition_dates("bronze/airnow/")

    assert result == {"2026-03-10", "2026-03-11"}


def test_raw_to_bronze_reads_json_and_writes_parquet(monkeypatch):
    shared = {}
    monkeypatch.setattr(etl_job, "streamflow_bucket", "test-bucket")
    monkeypatch.setattr(etl_job, "get_or_create_session", lambda: FakeSpark(shared))

    etl_job.raw_to_bronze()

    assert shared["raw_json_path"] == "s3a://test-bucket/landing/airnow/"
    
    assert any(
        call["path"] == "s3a://test-bucket/bronze/airnow/" and call["mode"] == "append" and call["partitionBy"] == ("date",)
        for call in shared["write_paths"]
    )


def test_bronze_to_silver_executes_transform_and_writes(monkeypatch):
    shared = {}
    monkeypatch.setattr(etl_job, "F", FakeFunctions)
    monkeypatch.setattr(etl_job, "dev", "0")
    monkeypatch.setattr(etl_job, "streamflow_bucket", "test-bucket")
    monkeypatch.setattr(etl_job, "get_partition_dates", lambda prefix: {"2026-03-10"} if prefix == "bronze/airnow/" else set())
    monkeypatch.setattr(etl_job, "get_or_create_session", lambda: FakeSpark(shared))

    etl_job.bronze_to_silver()

    assert shared["parquet_read_path"] == "s3a://test-bucket/bronze/airnow/date=2026-03-10"
    assert shared.get("filter_called", False) is True
    assert "FullAQSCode" in shared.get("drop_columns", [])
    assert "UTC" in shared.get("drop_columns", [])
    assert "composite_key" in shared.get("with_columns", [])

    silver_path = "s3a://test-bucket/silver/airnow_clean/date=2026-03-10"
    assert any(
        call["path"] == silver_path and call["mode"] == "append"
        for call in shared["write_paths"]
    )


def test_silver_to_gold_builds_and_writes_dimensions_facts(monkeypatch):
    shared = {}
    monkeypatch.setattr(etl_job, "F", FakeFunctions)
    monkeypatch.setattr(etl_job, "get_or_create_session", lambda: FakeSpark(shared))

    etl_job.silver_to_gold()

    assert shared["parquet_read_path"] == "s3a://stream-analytics-project-bucket/silver/airnow_clean"

    expected_write_paths = {
        "s3a://stream-analytics-project-bucket/gold/dim_site/",
        "s3a://stream-analytics-project-bucket/gold/dim_parameter/",
        "s3a://stream-analytics-project-bucket/gold/dim_date/",
        "s3a://stream-analytics-project-bucket/gold/dim_category/",
        "s3a://stream-analytics-project-bucket/gold/fact_air_quality_readings/",
    }

    actual_paths = {c["path"] for c in shared.get("write_paths", [])}
    assert expected_write_paths.issubset(actual_paths)
