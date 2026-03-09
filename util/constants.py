"""
Constants for project
"""

RAW_HISTORICAL_DATA_KAFKA_TOPIC = "airnow_raw_historical"
DOCKER_KAFKA_BOOTSTRAP_SERVER = ["kafka:9092"]
LOCAL_KAFKA_BOOTSTRAP_SERVER = ["localhost:9094"]
KAFKA_BATCH_SIZE = 100        # Messages per batch
KAFKA_BATCH_INTERVAL = 5     # Seconds before writing batch even if not full
MAX_KAFKA_CONSUMER_IDLE_TIME = 10  # seconds before exiting kafka consumer task

DOCKER_MINIO_ENDPOINT = "http://minio:9000"
LOCAL_MINIO_ENDPOINT = "http://localhost:9000"
MINIO_HISTORICAL_DATA_BUCKET = "kafka-raw-historical-data"
