from scripts import airnow_raw_producers, ingest_kafka_to_landing
from jobs import etl_job
from util import constants

def main():
    start = input("Enter the starting date: ")
    end = input("Enter the ending date: ")
    for bbox in constants.BBOXES:
        try:
            records = airnow_raw_producers.fetch_month_data(start, end, bbox)
            airnow_raw_producers.publish_raw_historical_records(records)
        except Exception as e:
            print(f"Failed at {bbox} for time period {start} - {end}")
            print("Failure due to the following error:\n", e)
    ingest_kafka_to_landing.consume_historical_data()
    etl_job.raw_to_bronze()
    etl_job.bronze_to_silver()
    etl_job.silver_to_gold()


if __name__ == "__main__":
    main()