import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px


# Streamlit app for displaying AirNow analysis
st.set_page_config(page_title="Air Quality Dashboard", layout="wide")
st.title("Air Quality Dashboard")

# --------------------------------------------------
# Load data
# --------------------------------------------------
df = pd.read_json("data/landing/testdata2.json")

# Convert UTC to datetime if needed
if "UTC" in df.columns:
    df["UTC"] = pd.to_datetime(df["UTC"])

# Clean -999 values in all numeric columns
numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
for col in numeric_cols:
    df[col] = df[col].replace(-999, np.nan)

# --------------------------------------------------
# 🔥 Derived time fields (needed for heatmaps)
# --------------------------------------------------
df["hour"] = df["UTC"].dt.hour
df["date"] = df["UTC"].dt.date

# --------------------------------------------------
# Axis schema (based on your real JSON)
# --------------------------------------------------
AXIS_OPTIONS = {
    "numeric": [
        "AQI",
        "Category",
        "Latitude",
        "Longitude"
    ],
    "temporal": [
        "UTC"
    ],
    "temporal_extended": [
        "hour",
        "date"
    ],
    "categorical": [
        "Category",
        "Parameter",
        "Unit",
        "SiteName",
        "AgencyName",
        "FullAQSCode",
        "IntlAQSCode"
    ]
}

# --------------------------------------------------
# Chart → allowed axis *types*
# --------------------------------------------------
CHART_AXIS_TYPES = {
    "Line": {
        "x": ["temporal"],
        "y": ["numeric"]
    },
    "Scatter": {
        "x": ["numeric", "temporal"],
        "y": ["numeric"]
    },
    "Bar": {
        "x": ["categorical"],
        "y": ["numeric"]
    },
    "Box": {
        "x": ["categorical"],
        "y": ["numeric"]
    },
    "Histogram": {
        "x": ["numeric"],
        "y": []
    },
    "Pie": {
        "x": ["categorical"],
        "y": []
    },
    "Pollution Map": {
        "x": ["numeric"],
        "y": []
    },
    # 🔥 Heatmap added
    "Heatmap": {
        "x": ["temporal_extended", "categorical"],
        "y": ["temporal_extended", "categorical"]
    }
}

# --------------------------------------------------
# Sidebar controls
# --------------------------------------------------
st.sidebar.header("Graph Controls")

chart_type = st.sidebar.selectbox(
    "Chart Type",
    list(CHART_AXIS_TYPES.keys())
)

def allowed_columns(axis_type_list):
    cols = []
    for axis_type in axis_type_list:
        cols.extend(AXIS_OPTIONS.get(axis_type, []))
    return cols

# Filter axis options based on chart type
x_options = allowed_columns(CHART_AXIS_TYPES[chart_type]["x"])
y_options = allowed_columns(CHART_AXIS_TYPES[chart_type]["y"])

x_axis = st.sidebar.selectbox("X-axis", x_options)

y_axis = None
if y_options:
    y_axis = st.sidebar.selectbox("Y-axis", y_options)

# --------------------------------------------------
# Plotting
# --------------------------------------------------
df_plot = df.copy()

if y_axis:
    df_plot = df_plot.dropna(subset=[y_axis])

st.subheader(f"{chart_type} Chart")

# ---------------- Line ----------------
if chart_type == "Line":

    agg_options = ["Average", "Max"]
    df_agg = st.sidebar.selectbox("Aggregation Options", agg_options)
    
    regions = ["All"] + sorted(df_plot["SiteName"].dropna().unique().tolist())
    selected_region = st.sidebar.selectbox("Select Region", regions)

    if selected_region != "All":
        df_plot = df_plot[df_plot["SiteName"] == selected_region]
        
    if df_agg == "Average":
        df_line = (
            df_plot
            .dropna(subset=["AQI"])
            .groupby("UTC", as_index=False)["AQI"]
            .mean()
            .sort_values("UTC")
        )
    else:
        df_line = (
            df_plot
            .dropna(subset=["AQI"])
            .groupby("UTC", as_index=False)["AQI"]
            .max()
            .sort_values("UTC")
        )

    st.line_chart(df_line.set_index(x_axis)[y_axis])

# ---------------- Scatter ----------------
elif chart_type == "Scatter":
    fig = px.scatter(
        df_plot,
        x=x_axis,
        y=y_axis,
        color="Parameter" if "Parameter" in df_plot.columns else None
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------------- Bar ----------------
elif chart_type == "Bar":
    agg = (
        df_plot
        .groupby(x_axis, as_index=False)[y_axis]
        .mean()
    )
    st.bar_chart(agg.set_index(x_axis))

# ---------------- Box ----------------
elif chart_type == "Box":
    fig = px.box(
        df_plot,
        x=x_axis,
        y=y_axis,
        color=x_axis
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------------- Histogram ----------------
elif chart_type == "Histogram":
    fig = px.histogram(df_plot, x=x_axis)
    st.plotly_chart(fig, use_container_width=True)

# ---------------- Pie ----------------
elif chart_type == "Pie":
    counts = df_plot[x_axis].value_counts().reset_index()
    counts.columns = [x_axis, "Count"]

    fig = px.pie(
        counts,
        names=x_axis,
        values="Count",
        title=f"{x_axis} Distribution"
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------------- Pollution Map ----------------
elif chart_type == "Pollution Map":
    if "UTC" in df_plot.columns:
        df_plot["UTC"] = pd.to_datetime(df_plot["UTC"])
        df_plot["Date"] = df_plot["UTC"].dt.date      # Extract date
        df_plot["Hour"] = df_plot["UTC"].dt.hour      # Extract hour
    # Date slider
    min_date = df_plot["Date"].min()
    max_date = df_plot["Date"].max()
    selected_date = st.sidebar.date_input(
        "Select Date",
        value=min_date,
        min_value=min_date,
        max_value=max_date
    )

    # Hour slider (0–23)
    selected_hour = st.sidebar.slider(
        "Select Hour of Day (0–23 UTC)",
        min_value=0,
        max_value=23,
        value=0
    )

    # Filter dataframe by both date and hour
    df_plot_filtered = df_plot[
        (df_plot["Date"] == selected_date) &
        (df_plot["Hour"] == selected_hour)
    ]

    df_map = df_plot_filtered.dropna(subset=["Latitude", "Longitude", "AQI"])

    fig_map = px.scatter_mapbox(
        df_map,
        lat="Latitude",
        lon="Longitude",
        color="AQI",
        size="AQI",
        color_continuous_scale="OrRd",
        size_max=15,
        zoom=3,
        hover_name="SiteName"
    )

    fig_map.update_layout(mapbox_style="open-street-map")
    fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig_map, use_container_width=True)

# ---------------- 🔥 Heatmap ----------------
elif chart_type == "Heatmap":
    heat_df = (
        df_plot
        .dropna(subset=["AQI"])
        .groupby([x_axis, y_axis], as_index=False)["AQI"]
        .mean()
    )

    fig = px.density_heatmap(
        heat_df,
        x=x_axis,
        y=y_axis,
        z="AQI",
        color_continuous_scale="Viridis"
    )

    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# Data preview
# --------------------------------------------------
with st.expander("Show raw data"):
    st.dataframe(df)