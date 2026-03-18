import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()

# --------------------------------------------------
# App config
# --------------------------------------------------
st.set_page_config(page_title="Air Quality Dashboard", layout="wide")
st.title("Air Quality Dashboard")

# # --------------------------------------------------
# # Load data
# # --------------------------------------------------
def load_data():
    base_path = "s3://stream-analytics-project-bucket/gold/"

    storage_options = {
        "key": os.getenv("MINIO_ROOT_USER"),
        "secret": os.getenv("MINIO_ROOT_PASSWORD"),
        "client_kwargs": {
            "endpoint_url": "http://localhost:9000"
        }
    }

    # fact = pd.read_parquet(base_path + "fact_air_quality_readings/", storage_options=storage_options)
    # site = pd.read_parquet(base_path + "dim_site/", storage_options=storage_options)
    # param = pd.read_parquet(base_path + "dim_parameter/", storage_options=storage_options)
    # date = pd.read_parquet(base_path + "dim_date/", storage_options=storage_options)
    # category = pd.read_parquet(base_path + "dim_category/", storage_options=storage_options)

    df = pd.read_parquet(base_path + "gold_df/", storage_options=storage_options)

    return df

df = load_data()


# --------------------------------------------------
# Axis schema (MATCHES YOUR DATA)
# --------------------------------------------------
AXIS_OPTIONS = {
    "numeric": ["aqi", "latitude", "longitude", "hour"],
    "temporal": ["date"],
    "categorical": [
        "parameter",
        "sitename",
        "agencyname",
        "unit",
        "concern_level"
    ]
}

# --------------------------------------------------
# Chart → allowed axis *types*
# --------------------------------------------------
CHART_AXIS_TYPES = {
    "Line": {"x": ["temporal"], "y": ["numeric"]},
    "Scatter": {"x": ["numeric"], "y": ["numeric"]},
    "Bar": {"x": ["categorical"], "y": ["numeric"]},
    "Box": {"x": ["categorical"], "y": ["numeric"]},
    "Histogram": {"x": ["numeric"], "y": []},
    "Pie": {"x": ["categorical"], "y": []},
    "Pollution Map": {"x": [], "y": []},
    "Heatmap": {"x": ["temporal", "categorical"], "y": ["categorical", "numeric"]}
}

# --------------------------------------------------
# Sidebar controls
# --------------------------------------------------
st.sidebar.header("Graph Controls")

chart_type = st.sidebar.selectbox("Chart Type", CHART_AXIS_TYPES.keys())

def allowed_columns(axis_types):
    cols = []
    for t in axis_types:
        cols.extend(AXIS_OPTIONS.get(t, []))
    return cols

x_options = allowed_columns(CHART_AXIS_TYPES[chart_type]["x"])
y_options = allowed_columns(CHART_AXIS_TYPES[chart_type]["y"])

x_axis = st.sidebar.selectbox("X-axis", x_options) if x_options else None
y_axis = st.sidebar.selectbox("Y-axis", y_options) if y_options else None

# Parameter
parameter_options = df["parameter"].unique().tolist()
selected_parameter = st.sidebar.selectbox("Select Parameter", ["All"] + parameter_options)

# US Regions by lat/lon
US_REGIONS = {
    "Northeast": {"lat_min": 36.5, "lat_max": 47.5, "lon_min": -80, "lon_max": -66},
    "Midwest":   {"lat_min": 36.5, "lat_max": 49.5, "lon_min": -104, "lon_max": -80},
    "South":     {"lat_min": 25,   "lat_max": 36.5, "lon_min": -105, "lon_max": -75},
    "West":      {"lat_min": 31,   "lat_max": 49.5, "lon_min": -125, "lon_max": -104},
    "Alaska":    {"lat_min": 51,   "lat_max": 71,    "lon_min": -170, "lon_max": -130},
    "Hawaii":    {"lat_min": 18.5, "lat_max": 22.5,  "lon_min": -161, "lon_max": -154}
}

region_options = ["All"] + list(US_REGIONS.keys())
selected_region = st.sidebar.selectbox("Select US Region", region_options)


if selected_parameter != "All":
    df = df[df["parameter"] == selected_parameter]

if selected_region != "All":
    region = US_REGIONS[selected_region]
    df = df[
        (df["latitude"] >= region["lat_min"]) &
        (df["latitude"] <= region["lat_max"]) &
        (df["longitude"] >= region["lon_min"]) &
        (df["longitude"] <= region["lon_max"])
    ]

st.subheader(f"{chart_type}")

# --------------------------------------------------
# Line: Average AQI by Date
# --------------------------------------------------
if chart_type == "Line":
    df_line = (
        df
        .groupby("date", as_index=False)["aqi"]
        .mean()
        .sort_values("date")
    )
    fig = px.line(
        df_line,
        x=x_axis,
        y=y_axis,
    )

    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Scatter
# --------------------------------------------------
elif chart_type == "Scatter":
    fig = px.scatter(
        df,
        x=x_axis,
        y=y_axis,
        color="parameter"
    )
    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Bar
# --------------------------------------------------
elif chart_type == "Bar":
    agg = (
        df
        .groupby(x_axis, as_index=False)["aqi"]
        .mean()
    )
    fig = px.bar(
        agg,
        x=x_axis,
        y=y_axis,
    )

    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Box
# --------------------------------------------------
elif chart_type == "Box":
    fig = px.box(df, x=x_axis, y="aqi", color=x_axis)
    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Histogram
# --------------------------------------------------
elif chart_type == "Histogram":
    fig = px.histogram(df, x=x_axis)
    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Pie
# --------------------------------------------------
elif chart_type == "Pie":
    counts = df[x_axis].value_counts().reset_index()
    counts.columns = [x_axis, "count"]
    fig = px.pie(counts, names=x_axis, values="count")
    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Pollution Map
# --------------------------------------------------
elif chart_type == "Pollution Map":
    min_date = df["date"].min()
    max_date = df["date"].max()
    selected_date = st.sidebar.date_input(
        "Date",
        value=min_date,
        min_value=min_date,
        max_value=max_date
    )

    selected_hour = st.sidebar.slider("Hour (UTC)", 0, 23, 0)

    df_map = df[
        (df["date"] == selected_date) &
        (df["hour"] == selected_hour)
    ].groupby(
        ["sitename", "latitude", "longitude"],
        as_index=False
    )["aqi"].mean()

    aqi_colorscale = [
        # Good (0–50)
        [0.00, "green"],
        [0.10, "green"],

        # Moderate (51–100)
        [0.10, "yellow"],
        [0.20, "yellow"],

        # Unhealthy for Sensitive Groups (101–150)
        [0.20, "orange"],
        [0.30, "orange"],

        # Unhealthy (151–200)
        [0.30, "red"],
        [0.40, "red"],

        # Very Unhealthy (201–300)
        [0.40, "purple"],
        [0.60, "purple"],

        # Hazardous (301–higher)
        [0.60, "maroon"],
        [1.00, "maroon"]
    ]

    fig = px.scatter_mapbox(
        df_map,
        lat="latitude",
        lon="longitude",
        color="aqi",
        size="aqi",
        zoom=4,
        hover_name="sitename",
        color_continuous_scale=aqi_colorscale
    )

    fig.update_layout(
        coloraxis=dict(
            cmin=0,
            cmax=500,
            colorbar=dict(
                title="AQI",
                tickvals=[0, 51, 101, 151, 201, 301, 500],
                ticktext=[
                    "0 Good",
                    "51 Moderate",
                    "101 Unhealthy for Sensitive Groups",
                    "151 Unhealthy",
                    "201 Very Unhealthy",
                    "301 Hazardous",
                    "500 Hazardous"
                ]
            )
        )
    )

    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Heatmap
# --------------------------------------------------
elif chart_type == "Heatmap":
    heat_df = (
        df
        .groupby([x_axis, y_axis], as_index=False)["aqi"]
        .mean()
    )

    fig = px.density_heatmap(
        heat_df,
        x=x_axis,
        y=y_axis,
        z="aqi",
        color_continuous_scale="Viridis"
    )
    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Data preview
# --------------------------------------------------
with st.expander("Show raw data"):
    st.dataframe(df)