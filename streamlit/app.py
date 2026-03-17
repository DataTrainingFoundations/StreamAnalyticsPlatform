import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json

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
dates2 = pd.date_range("2026-03-08", periods=52, freq="W")

seasonal_dataframe = pd.DataFrame({
    "AQI": np.random.randint(40, 120, size=52),
    "PM2.5": np.random.uniform(10, 35, size=52),
    "PM10": np.random.uniform(15, 50, size=52)
}, index=dates2)

st.subheader("Pollutant Trends Over Time")
pollutant = st.selectbox("Select pollutant", ["AQI", "PM2.5", "PM10"])
st.line_chart(seasonal_dataframe[pollutant])

corr_dataframe = pd.DataFrame({
    "AQI":   np.random.randint(40, 120, size=500),
    "PM2.5": np.random.uniform(10, 35, size=500),
    "PM10":  np.random.uniform(15, 50, size=500),
    "O3":    np.random.uniform(20, 80, size=500),
    "NO2":   np.random.uniform(5, 40, size=500),
})

st.subheader("Pollutant Correlation Heatmap")

# Create correlation matrix
corr_matrix = corr_dataframe.corr().round(2)

# Create heatmap
fig, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(corr_matrix, 
            annot=True, 
            cmap='RdYlBu_r', 
            center=0,
            vmin=-1, 
            vmax=1,
            square=True,
            ax=ax)
plt.title('Pollutant Correlation Matrix')
st.pyplot(fig)

landing_path = "data/landing"
frames = []
for filename in ["data.json", "testdata.json", "testdata2.json"]:
    filepath = os.path.join(landing_path, filename)
    with open(filepath) as f:
        frames.append(pd.DataFrame(json.load(f)))
 
df = pd.concat(frames, ignore_index=True)
 
# Pivot so each Parameter becomes a column of AQI values
corr_df = df.pivot_table(
    index=["SiteName", "UTC"],
    columns="Parameter",
    values="AQI",
    aggfunc="mean"
).reset_index(drop=True)
 
# Correlation heatmap
st.subheader("Pollutant Correlation Heatmap")
 
corr_matrix = corr_df.corr().round(2)
 
fig, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(corr_matrix,
            annot=True,
            cmap="RdYlBu_r",
            center=0,
            vmin=-1,
            vmax=1,
            square=True,
            ax=ax)
plt.title("Pollutant Correlation Matrix")
st.pyplot(fig)


# Average AQI by hour of day for a selected site
st.subheader("Average AQI by Hour of Day")

df["UTC"] = pd.to_datetime(df["UTC"])
df["hour"] = df["UTC"].dt.hour

min_records = st.slider("Minimum records required to show site", 1, 100, 24)

site_counts = df["SiteName"].value_counts()
qualified_sites = sorted(site_counts[site_counts >= min_records].index.tolist())

if not qualified_sites:
    st.warning(f"No sites have at least {min_records} records. Try lowering the threshold.")
else:
    site = st.selectbox("Select site", qualified_sites)

    hourly_avg = (
        df[df["SiteName"] == site]
        .groupby("hour")["AQI"]
        .mean()
        #.round(1)
        .reset_index()
    )

    st.bar_chart(hourly_avg.set_index("hour")["AQI"])

