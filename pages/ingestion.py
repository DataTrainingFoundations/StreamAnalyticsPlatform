import subprocess

import numpy as np
import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv

from jobs import etl_job
from scripts import airnow_raw_producers, cleanup_data, ingest_kafka_to_landing
from util import constants


load_dotenv()

st.set_page_config(page_title="Data Ingestion", layout="wide")
st.title("Data Ingestion Page")

start = st.text_input(
    "Start datetime (UTC)",
    value="2026-01-01T00",
    help="Format: YYYY-MM-DDTHH"
)

end = st.text_input(
    "End datetime (UTC)",
    value="2026-01-02T00",
    help="Format: YYYY-MM-DDTHH"
)

# --------------------------------------------------
# BBOX input (all or one)
# --------------------------------------------------
bbox_input = st.text_input(
    "Optional single BBOX (leave empty to ingest ALL)",
    value="",
    help="Format: minLon,minLat,maxLon,maxLat"
)

with st.expander("Show all available BBOXES"):
    st.write(constants.BBOXES)

# Determine which BBOXES to process
if bbox_input.strip() == "":
    bboxes_to_process = constants.BBOXES
    st.info("No BBOX provided → ingesting ALL regions")
else:
    bboxes_to_process = [bbox_input.strip()]
    st.info(f"Ingesting single BBOX: {bbox_input}")

if st.button("Run Ingestion Pipeline"):

    progress = st.progress(0)
    status = st.empty()

    try:
        for i, bbox in enumerate(bboxes_to_process):
            status.write(f"Fetching data for BBOX: `{bbox}`")

            records = airnow_raw_producers.fetch_month_data(
                start=start,
                end=end,
                bbox=bbox
            )

            airnow_raw_producers.publish_raw_historical_records(
                records,
                kafka_topic="kafka_raw_custom"
            )

            progress.progress((i + 1) / len(bboxes_to_process))

        progress.progress(1 / 5)
        status.write("Consuming Kafka → Landing")
        ingest_kafka_to_landing.consume_data("kafka_raw_custom")
        
        progress.progress(2 / 5)
        status.write("ETL: Raw → Bronze")
        etl_job.raw_to_bronze()
        
        progress.progress(3 / 5)
        status.write("ETL: Bronze → Silver")
        etl_job.bronze_to_silver()

        progress.progress(4 / 5)
        status.write("ETL: Silver → Gold")
        etl_job.silver_to_gold()

        progress.progress(5 / 5)
        status.write("Archiving processed files")
        cleanup_data.move_processed_data(
            os.getenv("STREAMFLOW_BUCKET_LANDING_PREFIX"),
            os.getenv("STREAMFLOW_BUCKET_ARCHIVE_PREFIX")
        )

        st.success("Ingestion completed successfully!")

    except Exception as e:
        st.error("Ingestion failed")
        st.exception(e)

