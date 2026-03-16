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

# --------------------------------------------------
# Load data
# --------------------------------------------------
df = pd.read_parquet(
    "s3a://stream-analytics-project-bucket/silver/airnow_clean",
    storage_options={
        "key": os.getenv("AWS_USER"),
        "secret": os.getenv("AWS_PASSWORD")
    }
)
df["date"] = pd.to_datetime(df["date"].astype(str))
# --------------------------------------------------
# Clean AQI (-999 → NaN)
# --------------------------------------------------
df["aqi"] = df["aqi"].replace(-999, np.nan)

# Ensure correct dtypes
df["hour"] = df["hour"].astype(int)

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
        "concern_level",
        "intlaqscode"
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

# --------------------------------------------------
# Filter invalid AQI values
# --------------------------------------------------
df_plot = df.dropna(subset=["aqi"])

st.subheader(f"{chart_type}")

# --------------------------------------------------
# Line: Average AQI by Date
# --------------------------------------------------
if chart_type == "Line":
    df_line = (
        df_plot
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
        df_plot,
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
        df_plot
        .groupby(x_axis, as_index=False)["aqi"]
        .mean()
    )
    fig = px.bar(
        df_plot,
        x=x_axis,
        y=y_axis,
    )

    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Box
# --------------------------------------------------
elif chart_type == "Box":
    fig = px.box(df_plot, x=x_axis, y="aqi", color=x_axis)
    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Histogram
# --------------------------------------------------
elif chart_type == "Histogram":
    fig = px.histogram(df_plot, x=x_axis)
    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Pie
# --------------------------------------------------
elif chart_type == "Pie":
    counts = df_plot[x_axis].value_counts().reset_index()
    counts.columns = [x_axis, "count"]
    fig = px.pie(counts, names=x_axis, values="count")
    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Pollution Map
# --------------------------------------------------
elif chart_type == "Pollution Map":
    min_date = df_plot["date"].min().date()
    max_date = df_plot["date"].max().date()
    selected_date = st.sidebar.date_input(
        "Date",
        value=min_date,
        min_value=min_date,
        max_value=max_date
    )

    selected_hour = st.sidebar.slider("Hour (UTC)", 0, 23, 0)

    df_map = df_plot[
        (df_plot["date"].dt.date == selected_date) &
        (df_plot["hour"] == selected_hour)
    ]

    fig = px.scatter_mapbox(
        df_map,
        lat="latitude",
        lon="longitude",
        color="aqi",
        size="aqi",
        zoom=4,
        hover_name="sitename",
        color_continuous_scale="OrRd"
    )

    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Heatmap
# --------------------------------------------------
elif chart_type == "Heatmap":
    heat_df = (
        df_plot
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