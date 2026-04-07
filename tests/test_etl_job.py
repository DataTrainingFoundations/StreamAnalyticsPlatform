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
        self.columns = ["date", "hour", "IntlAQSCode", "Parameter", "Category", "FullAQSCode", "UTC", "intlaqscode", "sitename", "agencyname", "latitude", "longitude", "aqi", "parameter", "unit"]

    def filter(self, *_args, **_kwargs):
        self.shared["filter_called"] = True
        return self

    def withColumn(self, name, *_args, **_kwargs):
        self.shared.setdefault("with_columns", []).append(name)
        return self

    def drop(self, *col_names):
        self.shared.setdefault("drop_columns", []).extend(col_names)
        return self

    def dropDuplicates(self, columns):
        self.shared["dropDuplicates"] = columns
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

    def repartition(self, *cols):
        self.shared.setdefault("repartition", []).append(cols)
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


class FakeHadoopPath:
    def __init__(self, path):
        self.path = path

    def toString(self):
        return self.path


class FakeHadoopStatus:
    def __init__(self, path):
        self._path = FakeHadoopPath(path)

    def getPath(self):
        return self._path


class FakeHadoopFS:
    def __init__(self, status_list=None):
        self.status_list = status_list or []

    def listStatus(self, path):
        path_str = path.toString() if hasattr(path, "toString") else str(path)
        if "landing/airnow/" in path_str and "date=" not in path_str:
            return [FakeHadoopStatus(path_str + "date=2026-03-10")]
        if "date=2026-03-10" in path_str:
            return [FakeHadoopStatus(path_str + "/hour=01")]
        return []


class FakeSpark:
    def __init__(self, shared):
        self.shared = shared
        self.read = self
        self._jsc = self
        self._jvm = self

    def hadoopConfiguration(self):
        return {}

    class java:
        class net:
            @staticmethod
            def URI(path):
                return path

    class org:
        class apache:
            class hadoop:
                class fs:
                    class FileSystem:
                        @staticmethod
                        def get(uri, _conf):
                            return FakeHadoopFS()

                    @staticmethod
                    def Path(path):
                        return FakeHadoopPath(path)

    def schema(self, *args, **kwargs):
        self.shared["schema_called"] = True
        return self

    def option(self, key, value):
        self.shared.setdefault("options", []).append((key, value))
        return self

    def parquet(self, *paths):
        self.shared["parquet_read_path"] = paths
        return FakeDataFrame(self.shared)

    def json(self, path):
        self.shared["raw_json_path"] = path
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

    def __getitem__(self, _key):
        return self

    def alias(self, _name):
        return self


class FakeFunctions:
    @staticmethod
    def col(_name):
        return FakeColumn(f"col({_name})")

    @staticmethod
    def input_file_name():
        return FakeColumn("input_file_name")

    @staticmethod
    def regexp_extract(col, _pattern, _idx):
        return FakeColumn("regexp_extract")

    @staticmethod
    def create_map(items):
        return FakeColumn("create_map")

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
    def to_date(_col, _fmt=None):
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
    monkeypatch.setattr(etl_job, "F", FakeFunctions)
    monkeypatch.setattr(etl_job, "get_or_create_session", lambda: FakeSpark(shared))

    etl_job.raw_to_bronze()

    assert shared["raw_json_path"] == ["s3a://test-bucket/landing/airnow/date=2026-03-10/hour=01"]

    assert any(
        call["path"] == "s3a://test-bucket/bronze/airnow/" and call["mode"] == "append" and call["partitionBy"] == ("date", "hour")
        for call in shared.get("write_paths", [])
    )


def test_bronze_to_silver_no_new_dates_returns(monkeypatch):
    shared = {}
    monkeypatch.setattr(etl_job, "F", FakeFunctions)
    monkeypatch.setattr(etl_job, "dev", "0")
    monkeypatch.setattr(etl_job, "streamflow_bucket", "test-bucket")
    monkeypatch.setattr(etl_job, "get_partition_dates", lambda prefix: set())
    monkeypatch.setattr(etl_job, "get_or_create_session", lambda: FakeSpark(shared))

    etl_job.bronze_to_silver()

    assert "parquet_read_path" not in shared
    assert "write_paths" not in shared


def test_silver_to_gold_no_new_dates_returns(monkeypatch):
    shared = {}
    monkeypatch.setattr(etl_job, "F", FakeFunctions)
    monkeypatch.setattr(etl_job, "get_partition_dates", lambda prefix: set())
    monkeypatch.setattr(etl_job, "get_or_create_session", lambda: FakeSpark(shared))

    etl_job.silver_to_gold()

    assert "parquet_read_path" not in shared
    assert "write_paths" not in shared


def test_bronze_to_silver_transforms_and_writes(monkeypatch):
    shared = {}
    monkeypatch.setattr(etl_job, "F", FakeFunctions)
    monkeypatch.setattr(etl_job, "dev", "0")
    monkeypatch.setattr(etl_job, "streamflow_bucket", "test-bucket")
    monkeypatch.setattr(etl_job, "get_partition_dates", lambda prefix: {"2026-03-10"} if prefix == "bronze/airnow/" else set())
    monkeypatch.setattr(etl_job, "get_or_create_session", lambda: FakeSpark(shared))

    etl_job.bronze_to_silver()

    assert shared["parquet_read_path"] == ("s3a://test-bucket/bronze/airnow/date=2026-03-10",)
    assert "concern_level" in shared.get("with_columns", [])


def test_silver_to_gold_transforms_and_writes(monkeypatch):
    shared = {}
    monkeypatch.setattr(etl_job, "F", FakeFunctions)
    monkeypatch.setattr(etl_job, "dev", "0")
    monkeypatch.setattr(etl_job, "streamflow_bucket", "test-bucket")
    monkeypatch.setattr(etl_job, "get_partition_dates", lambda prefix: {"2026-03-10"} if prefix.startswith("silver") else set())
    monkeypatch.setattr(etl_job, "get_or_create_session", lambda: FakeSpark(shared))

    etl_job.silver_to_gold()

    assert shared["parquet_read_path"] == ("s3a://test-bucket/silver/airnow_clean/date=2026-03-10",)
    expected = {
        "s3a://test-bucket/gold/airnow/dim_site/",
        "s3a://test-bucket/gold/airnow/dim_parameter/",
        "s3a://test-bucket/gold/airnow/dim_date/",
        "s3a://test-bucket/gold/airnow/dim_category/",
        "s3a://test-bucket/gold/airnow/fact_air_quality_readings/",
    }
    actual = {c["path"] for c in shared.get("write_paths", [])}
    assert expected.issubset(actual)


def test_process_date_transforms_and_writes(monkeypatch):
    shared = {}
    monkeypatch.setattr(etl_job, "streamflow_bucket", "test-bucket")
    monkeypatch.setattr(etl_job, "F", FakeFunctions)

    etl_job.process_date(["s3a://test-bucket/landing/airnow/date=2026-03-10/hour=01"], FakeSpark(shared))

    assert any(call["path"] == "s3a://test-bucket/bronze/airnow/" for call in shared.get("write_paths", []))

