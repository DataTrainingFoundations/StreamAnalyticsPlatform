import streamlit as st
import pandas as pd
import numpy as np

"""
Streamlit app for displaying analysis
"""

st.title("Air Quality Dashboard")

dates = pd.date_range("2026-03-08", periods=50, freq="H")

aqi_dataframe = pd.DataFrame({
    "AQI": np.random.randint(40, 120, size=50),
    "PM2.5": np.random.uniform(10, 35, size=50),
    "PM10": np.random.uniform(15, 50, size=50)
}, index=dates)

st.metric("Current AQI", int(aqi_dataframe["AQI"].iloc[-1]))

st.line_chart(aqi_dataframe)