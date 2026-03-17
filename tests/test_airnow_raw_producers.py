import json
from datetime import datetime
from unittest.mock import patch, MagicMock
import pytest
import boto3
from moto import mock_aws

import scripts.airnow_raw_producers as producers

@mock_aws
def test_get_oldest_record_date_success(mock_env):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="progress.json",
        Body=json.dumps({
            "oldest_loaded_date": "2024-01-01T00:00"
        })
    )

    result = producers.get_oldest_record_date()
    assert isinstance(result, datetime)
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 1
    assert result.hour == 0
    assert result.minute == 0

@mock_aws
def test_get_oldest_record_date_no_key(mock_env):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    result = producers.get_oldest_record_date()

    assert result is None

@mock_aws
def test_get_oldest_record_date_no_bucket(mock_env):
    result = producers.get_oldest_record_date()
    assert result is None

@patch("scripts.airnow_raw_producers.boto3.client")
def test_get_oldest_record_date_missing_key(mock_boto):
    with patch.dict("os.environ", {
        "STREAMFLOW_BUCKET": "test-bucket",
        "DEV": "0",
        "AWS_USER": "test",
        "AWS_PASSWORD": "test",
        "AWS_REGION_NAME": "us-east-1",
        "STREAMFLOW_BUCKET_PROGRESS_KEY": "",
    }):
        with pytest.raises(RuntimeError):
            producers.get_oldest_record_date()

@patch("scripts.airnow_raw_producers.get_oldest_record_date", return_value=None)
def test_get_times_default(mock_oldest):
    start, end = producers.get_times()

    assert isinstance(start, str)
    assert isinstance(end, str)
    assert start < end
