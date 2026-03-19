"""
Cleanup Functions for Processed Data

This module defines cleanup functions for processed raw, bronze, and silver data
"""
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import boto3

load_dotenv()

docker_env = os.getenv("DOCKER_ENV")
dev = os.getenv("DEV")

def move_processed_data(source_prefix, dest_prefix, max_workers=5):
    """
    Move all objects from one prefix to another in S3/MinIO using parallel copy + batch deletes.
    Suitable for Airflow DAG tasks.

    Args:
        source_prefix (str): e.g., "landing/airnow/"
        dest_prefix (str): e.g., "archive/airnow/"
        max_workers (int): Number of parallel threads to use for copying
    """
    print(f"Starting move from '{source_prefix}' → '{dest_prefix}'")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_USER"),
        aws_secret_access_key=os.getenv("AWS_PASSWORD"),
        region_name="us-east-1",
    )

    streamflow_bucket = os.getenv("STREAMFLOW_BUCKET")
    paginator = s3_client.get_paginator("list_objects_v2")
    objects_to_delete = []

    def copy_object(obj):
        """Copy a single object and return its source key for deletion."""
        source_key = obj["Key"]
        dest_key = source_key.replace(source_prefix, dest_prefix, 1)
        s3_client.copy_object(
            Bucket=streamflow_bucket,
            CopySource={"Bucket": streamflow_bucket, "Key": source_key},
            Key=dest_key
        )
        return {"Key": source_key}

    # Paginate through all objects under the source prefix
    for page in paginator.paginate(Bucket=streamflow_bucket, Prefix=source_prefix):
        if "Contents" not in page:
            continue

        print(f"Processing {len(page['Contents'])} objects from current page...")

        # Parallel copy
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(copy_object, obj) for obj in page["Contents"]]

            for future in as_completed(futures):
                try:
                    objects_to_delete.append(future.result())
                except Exception as e:
                    print(f"Failed to copy object: {e}")

                # Batch delete when we reach 1000 objects
                if len(objects_to_delete) >= 1000:
                    print(f"Deleting batch of {len(objects_to_delete)} objects...")
                    s3_client.delete_objects(
                        Bucket=streamflow_bucket,
                        Delete={"Objects": objects_to_delete}
                    )
                    objects_to_delete = []

    # Delete remaining objects
    if objects_to_delete:
        print(f"Deleting final batch of {len(objects_to_delete)} objects...")
        s3_client.delete_objects(
            Bucket=streamflow_bucket,
            Delete={"Objects": objects_to_delete}
        )

    print(f"Move completed from '{source_prefix}' → '{dest_prefix}'")
if __name__ == "__main__":
    landing_prefix = os.getenv("STREAMFLOW_BUCKET_LANDING_PREFIX")
    archive_prefix = os.getenv("STREAMFLOW_BUCKET_ARCHIVE_PREFIX")
    move_processed_data(landing_prefix, archive_prefix)
