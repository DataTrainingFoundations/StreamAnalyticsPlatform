import os
from scripts import cleanup_data


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, Bucket, Prefix):
        assert Bucket == os.getenv("STREAMFLOW_BUCKET")
        return self.pages


class FakeS3Client:
    def __init__(self, pages):
        self.pages = pages
        self.copies = []
        self.deletes = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return FakePaginator(self.pages)

    def copy_object(self, Bucket, CopySource, Key):
        self.copies.append({"Bucket": Bucket, "CopySource": CopySource, "Key": Key})

    def delete_objects(self, Bucket, Delete):
        self.deletes.append({"Bucket": Bucket, "Delete": Delete})


def test_move_processed_data_no_objects(monkeypatch):
    monkeypatch.setenv("STREAMFLOW_BUCKET", "test-bucket")
    monkeypatch.setenv("DEV", "0")
    monkeypatch.setenv("DOCKER_ENV", "0")

    fake = FakeS3Client(pages=[{"IsTruncated": False, "Contents": []}])

    monkeypatch.setattr(cleanup_data.boto3, "client", lambda *args, **kwargs: fake)

    cleanup_data.move_processed_data("landing/", "archive/")

    assert fake.copies == []
    assert fake.deletes == []


def test_move_processed_data_moves_objects_and_deletes_batch(monkeypatch):
    monkeypatch.setenv("STREAMFLOW_BUCKET", "test-bucket")
    monkeypatch.setenv("DEV", "0")
    monkeypatch.setenv("DOCKER_ENV", "0")

    pages = [
        {"Contents": [{"Key": "landing/a.json"}, {"Key": "landing/b.json"}]}
    ]

    fake = FakeS3Client(pages=pages)

    monkeypatch.setattr(cleanup_data.boto3, "client", lambda *args, **kwargs: fake)

    cleanup_data.move_processed_data("landing/", "archive/")

    assert fake.copies == [
        {
            "Bucket": "test-bucket",
            "CopySource": {"Bucket": "test-bucket", "Key": "landing/a.json"},
            "Key": "archive/a.json",
        },
        {
            "Bucket": "test-bucket",
            "CopySource": {"Bucket": "test-bucket", "Key": "landing/b.json"},
            "Key": "archive/b.json",
        },
    ]

    assert len(fake.deletes) == 1
    assert fake.deletes[0]["Delete"]["Objects"] == [{"Key": "landing/a.json"}, {"Key": "landing/b.json"}]


def test_move_processed_data_deletes_in_thousand_batches(monkeypatch):
    monkeypatch.setenv("STREAMFLOW_BUCKET", "test-bucket")
    monkeypatch.setenv("DEV", "0")
    monkeypatch.setenv("DOCKER_ENV", "0")

    contents = [{"Key": f"landing/file_{i}.json"} for i in range(1005)]
    pages = [{"Contents": contents}]

    fake = FakeS3Client(pages=pages)
    monkeypatch.setattr(cleanup_data.boto3, "client", lambda *args, **kwargs: fake)

    cleanup_data.move_processed_data("landing/", "archive/")

    # 1 batch of 1000 + remaining 5
    assert len(fake.deletes) == 2
    assert len(fake.deletes[0]["Delete"]["Objects"]) == 1000
    assert len(fake.deletes[1]["Delete"]["Objects"]) == 5
