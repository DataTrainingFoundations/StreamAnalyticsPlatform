"""
Cleanup Functions for Processed Data

This module defines cleanup functions for processed raw, bronze, and silver data
"""
import os
from dotenv import load_dotenv
import boto3

load_dotenv()

docker_env = os.getenv("DOCKER_ENV")
dev = os.getenv("DEV")

def move_processed_data(source_prefix, dest_prefix):
    """
    Move all objects from one prefix to another using batch deletes.
    Works with S3 and MinIO.
    """
    s3_client = (
        boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_USER"),
            aws_secret_access_key=os.getenv("AWS_PASSWORD"),
            region_name=os.getenv("AWS_REGION_NAME"),
        )
        if dev != "1"
        else boto3.client(
            "s3",
            endpoint_url=(
                os.getenv("DOCKER_MINIO_ENDPOINT")
                if docker_env == "1"
                else os.getenv("LOCAL_MINIO_ENDPOINT")
            ),
            aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        )
    )

    streamflow_bucket = os.getenv("STREAMFLOW_BUCKET")

    paginator = s3_client.get_paginator("list_objects_v2")
    objects_to_delete = []

    for page in paginator.paginate(Bucket=streamflow_bucket, Prefix=source_prefix):

        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            source_key = obj["Key"]
            dest_key = source_key.replace(source_prefix, dest_prefix, 1)

            # Copy object
            s3_client.copy_object(
                Bucket=streamflow_bucket,
                CopySource={"Bucket": streamflow_bucket, "Key": source_key},
                Key=dest_key
            )

            # Queue for batch deletion
            objects_to_delete.append({"Key": source_key})

            # Delete in batches of 1000
            if len(objects_to_delete) == 1000:
                s3_client.delete_objects(
                    Bucket=streamflow_bucket,
                    Delete={"Objects": objects_to_delete}
                )
                objects_to_delete = []

    # Delete remaining objects
    if objects_to_delete:
        s3_client.delete_objects(
            Bucket=streamflow_bucket,
            Delete={"Objects": objects_to_delete}
        )

    print("Move completed.")

if __name__ == "__main__":
    archive_prefix = os.getenv("STREAMFLOW_BUCKET_ARCHIVE_PREFIX")
    landing_prefix = os.getenv("STREAMFLOW_BUCKET_LANDING_PREFIX")
    move_processed_data(landing_prefix, archive_prefix)
