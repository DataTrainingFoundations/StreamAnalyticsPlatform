import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px


#Streamlit app for displaying AirNow analysis


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
        cols.extend(AXIS_OPTIONS[axis_type])
    return cols

# Filter axis options based on chart type
x_options = allowed_columns(CHART_AXIS_TYPES[chart_type]["x"])
y_options = allowed_columns(CHART_AXIS_TYPES[chart_type]["y"])

x_axis = st.sidebar.selectbox("X-axis", x_options)

y_axis = None
if y_options:
    y_axis = st.sidebar.selectbox("Y-axis", y_options)

# --------------------------------------------------
# Plotting (no invalid states possible)
# --------------------------------------------------
df_plot = df.copy()

# Drop rows with missing Y values when Y is required
if y_axis:
    df_plot = df_plot.dropna(subset=[y_axis])

st.subheader(f"{chart_type} Chart")

if chart_type == "Line":
    agg_options = ["Average", "Max"]
    df_agg = st.sidebar.selectbox("Aggregation Options", agg_options)
    if df_agg == "Average":
        df_line = (
            df_plot
            .dropna(subset=["AQI"])
            .groupby("UTC", as_index=False)["AQI"]
            .mean()
            .sort_values("UTC")
        )
    elif df_agg == "Max":
        df_line = (
            df_plot
            .dropna(subset=["AQI"])
            .groupby("UTC", as_index=False)["AQI"]
            .max()
            .sort_values("UTC")
        )
    st.line_chart(df_line.set_index(x_axis)[y_axis])

elif chart_type == "Scatter":
    fig = px.scatter(
        df_plot,
        x=x_axis,
        y=y_axis,
        color="Parameter" if "Parameter" in df_plot.columns else None
    )
    st.plotly_chart(fig, use_container_width=True)

elif chart_type == "Bar":
    agg = (
        df_plot
        .groupby(x_axis, as_index=False)[y_axis]
        .mean()
    )
    st.bar_chart(agg.set_index(x_axis))

elif chart_type == "Box":
    fig = px.box(
        df_plot,
        x=x_axis,
        y=y_axis,
        color=x_axis
    )
    st.plotly_chart(fig, use_container_width=True)

elif chart_type == "Histogram":
    fig = px.histogram(
        df_plot,
        x=x_axis
    )
    st.plotly_chart(fig, use_container_width=True)
    
elif chart_type == "Pie":
    if x_axis in df_plot.columns:
        counts = df_plot[x_axis].value_counts().reset_index()
        counts.columns = [x_axis, "Count"]
        fig_pie = px.pie(
            counts,
            names=x_axis,
            values="Count",
            title=f"{x_axis} Distribution",
            color=x_axis,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.write(f"No column '{x_axis}' found in the data.")

elif chart_type == "Pollution Map":
    if {"Latitude", "Longitude", "AQI"}.issubset(df_plot.columns):
        df_map = df_plot.dropna(subset=["Latitude", "Longitude", "AQI"])
        
        fig_map = px.scatter_mapbox(
            df_map,
            lat="Latitude",
            lon="Longitude",
            color="AQI",
            size="AQI",
            color_continuous_scale=px.colors.sequential.OrRd,
            size_max=15,
            zoom=3,
            hover_name="SiteName" if "SiteName" in df_map.columns else None,
            hover_data={"Latitude": True, "Longitude": True, "AQI": True}
        )

        fig_map.update_layout(mapbox_style="open-street-map")
        fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.write("Latitude, Longitude, or AQI column missing.")
# --------------------------------------------------
# Optional: Data preview
# --------------------------------------------------
with st.expander("Show raw data"):
    st.dataframe(df)