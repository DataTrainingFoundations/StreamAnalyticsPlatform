from scripts import airnow_raw_producers, ingest_kafka_to_landing, cleanup_data
from jobs import etl_job
from util import constants
from dotenv import load_dotenv
import os

def main():
    load_dotenv()
    start = input("Enter the starting date (YYYY-MM-DDTHH): ")
    end = input("Enter the ending date (YYYY-MM-DDTHH): ")
    i = 0
    for bbox in constants.BBOXES:
        try:
            if i == 5:
                break
            records = airnow_raw_producers.fetch_month_data(start, end, bbox)
            airnow_raw_producers.publish_raw_historical_records(records, "kafka_raw_custom")
            i += 1
        except Exception as e:
            print(f"Failed at {bbox} for time period {start} - {end}")
            print("Failure due to the following error:\n", e)
    ingest_kafka_to_landing.consume_data("kafka_raw_custom")
    etl_job.raw_to_bronze()
    etl_job.bronze_to_silver()
    etl_job.silver_to_gold()
    cleanup_data.move_processed_data(os.getenv("STREAMFLOW_BUCKET_ARCHIVE_PREFIX"), os.getenv("STREAMFLOW_BUCKET_LANDING_PREFIX"))


if __name__ == "__main__":
    main()