import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import os
from dotenv import load_dotenv
import duckdb

load_dotenv()

# --------------------------------------------------
# App config
# --------------------------------------------------
st.set_page_config(page_title="Air Quality Dashboard", layout="wide")
st.title("Air Quality Dashboard")

# # --------------------------------------------------
# # Load data
# # --------------------------------------------------

# @st.cache_data(ttl=300)
# def load_data():
#     base_path = "s3://stream-analytics-project-bucket/silver/airnow_clean/"

#     storage_options = {
#         "key": os.getenv("AWS_USER"),
#         "secret": os.getenv("AWS_PASSWORD"),
#         "client_kwargs": {
#             "region_name": "us-east-1"
#         }
#     }

#     # fact = pd.read_parquet(base_path + "fact_air_quality_readings/", storage_options=storage_options)
#     # site = pd.read_parquet(base_path + "dim_site/", storage_options=storage_options)
#     # param = pd.read_parquet(base_path + "dim_parameter/", storage_options=storage_options)
#     # date = pd.read_parquet(base_path + "dim_date/", storage_options=storage_options)
#     # category = pd.read_parquet(base_path + "dim_category/", storage_options=storage_options)

#     df = pd.read_parquet(base_path, storage_options=storage_options)

#     # return fact, site, param, date, category
#     return df

os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_USER")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_PASSWORD")
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

con = duckdb.connect()

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

con.execute("SET s3_region='us-east-1';")
# fact, site, param, date, category = load_data()
df = con.execute("""
    SELECT avg(f.aqi), f.date
    FROM read_parquet(
        's3://stream-analytics-project-bucket/gold/airnow/fact_air_quality_readings/*/*/*.parquet'
    ) f
    JOIN read_parquet(
        's3://stream-analytics-project-bucket/gold/airnow/dim_parameter/*.parquet'
    ) p 
    ON f.parameter_key = p.parameter_key
    WHERE f.date BETWEEN '2025-12-01' AND '2026-02-28' AND p.parameter = 'PM2.5' 
    GROUP BY f.date
""").df()

with st.expander("show df"):
    st.dataframe(df)
# --------------------------------------------------
# Axis schema (MATCHES YOUR DATA)
# --------------------------------------------------
# AXIS_OPTIONS = {
#     "numeric": ["aqi"],
#     "temporal": ["date"],
#     "categorical": [
#         "parameter",
#         "sitename",
#         "agencyname",
#         "unit",
#         "concern_level",
#         "hour"
#     ],
# }

# # --------------------------------------------------
# # Chart → allowed axis *types*
# # --------------------------------------------------
# CHART_AXIS_TYPES = {
#     "Line": {"x": ["temporal"], "y": ["numeric"]},
#     "Bar": {"x": ["categorical"], "y": ["numeric"]},
#     "Box": {"x": ["categorical"], "y": ["numeric"]},
#     "Pie": {"x": ["categorical"], "y": []},
#     "Pollution Map": {"x": [], "y": []}
# }

# # --------------------------------------------------
# # Sidebar controls
# # --------------------------------------------------
# st.sidebar.header("Graph Controls")

# chart_type = st.sidebar.selectbox("Chart Type", CHART_AXIS_TYPES.keys())

# def allowed_columns(axis_types):
#     cols = []
#     for t in axis_types:
#         cols.extend(AXIS_OPTIONS.get(t, []))
#     return cols

# x_options = allowed_columns(CHART_AXIS_TYPES[chart_type]["x"])
# y_options = allowed_columns(CHART_AXIS_TYPES[chart_type]["y"])

# x_axis = st.sidebar.selectbox("X-axis", x_options) if x_options else None
# y_axis = st.sidebar.selectbox("Y-axis", y_options) if y_options else None

# # Parameter
# parameter_options = param["parameter"].unique().tolist()
# selected_parameter = st.sidebar.selectbox("Select Parameter", ["All"] + parameter_options)

# # US Regions by lat/lon
# US_REGIONS = {
#     "Northeast": {"lat_min": 36.5, "lat_max": 47.5, "lon_min": -80, "lon_max": -66},
#     "Midwest":   {"lat_min": 36.5, "lat_max": 49.5, "lon_min": -104, "lon_max": -80},
#     "South":     {"lat_min": 25,   "lat_max": 36.5, "lon_min": -105, "lon_max": -75},
#     "West":      {"lat_min": 31,   "lat_max": 49.5, "lon_min": -125, "lon_max": -104},
#     "Alaska":    {"lat_min": 51,   "lat_max": 71,    "lon_min": -170, "lon_max": -130},
#     "Hawaii":    {"lat_min": 18.5, "lat_max": 22.5,  "lon_min": -161, "lon_max": -154}
# }

# region_options = ["All"] + list(US_REGIONS.keys())
# selected_region = st.sidebar.selectbox("Select US Region", region_options)

# st.subheader(f"{chart_type}")

# # --------------------------------------------------
# # Line: Average AQI by Date
# # --------------------------------------------------
# if chart_type == "Line":
#     # Merge only the columns needed
#     df_line = fact.merge(date, on="date_key", how="left")
#     df_line = df_line.merge(site, on="site_key", how="left")
#     # st.write(df_line.columns)
#     if selected_parameter != "All":
#         allowed_keys = param[param["parameter"] == selected_parameter]["parameter_key"]
#         df_line = df_line[df_line["parameter_key"].isin(allowed_keys)]

#     # Filter by region
#     if selected_region != "All":
#         region = US_REGIONS[selected_region]
#         df_line = df_line[
#             (df_line["latitude"] >= region["lat_min"]) &
#             (df_line["latitude"] <= region["lat_max"]) &
#             (df_line["longitude"] >= region["lon_min"]) &
#             (df_line["longitude"] <= region["lon_max"])
#         ]

#     df_line = df_line.groupby("date_y", as_index=False)["aqi"].mean().sort_values("date_y")
    
#     # st.write(df_line.head())
#     fig = px.line(df_line, x="date_y", y=y_axis)
#     st.plotly_chart(fig, use_container_width=True)

# # --------------------------------------------------
# # Bar
# # --------------------------------------------------
# elif chart_type == "Bar":
#     # Merge only the hour column from date
#     df_bar = fact.merge(date[["date_key", "hour"]], on="date_key", how="left")

#     # Filter by parameter if selected
#     if selected_parameter != "All":
#         allowed_keys = param[param["parameter"] == selected_parameter]["parameter_key"]
#         df_bar = df_bar[df_bar["parameter_key"].isin(allowed_keys)]

#     # Filter by region if selected
#     if selected_region != "All":
#         df_bar = df_bar.merge(site[["site_key", "latitude", "longitude"]], on="site_key", how="left")
#         region = US_REGIONS[selected_region]
#         df_bar = df_bar[
#             (df_bar["latitude"] >= region["lat_min"]) &
#             (df_bar["latitude"] <= region["lat_max"]) &
#             (df_bar["longitude"] >= region["lon_min"]) &
#             (df_bar["longitude"] <= region["lon_max"])
#         ]

#     # Group by hour and compute average AQI
#     agg = df_bar.groupby("hour_y", as_index=False)["aqi"].mean()

#     fig = px.bar(
#         agg,
#         x="hour_y",
#         y="aqi",
#         labels={"hour_y": "Hour (UTC)", "aqi": "Average AQI"}
#     )

#     st.plotly_chart(fig, use_container_width=True)

# # --------------------------------------------------
# # Box
# # --------------------------------------------------
# elif chart_type == "Box":
#     # Merge minimal columns needed
#     df_box = fact.merge(
#         site[["site_key", "sitename"]],
#         on="site_key",
#         how="left"
#     ).merge(
#         param[["parameter_key", "parameter"]],
#         on="parameter_key",
#         how="left"
#     )

#     # Filter by region if selected
#     if selected_region != "All":
#         region = US_REGIONS[selected_region]
#         df_box = df_box.merge(site[["site_key", "latitude", "longitude"]], on="site_key")
#         df_box = df_box[
#             (df_box["latitude"] >= region["lat_min"]) &
#             (df_box["latitude"] <= region["lat_max"]) &
#             (df_box["longitude"] >= region["lon_min"]) &
#             (df_box["longitude"] <= region["lon_max"])
#         ]

#     # Filter by parameter
#     if selected_parameter != "All":
#         df_box = df_box[df_box["parameter"] == selected_parameter]

#     # Ensure x_axis exists in df_box
#     if x_axis not in df_box.columns:
#         st.warning(f"Column {x_axis} not found in data. Using 'parameter' instead.")
#         x_axis = "parameter"

#     fig = px.box(
#         df_box,
#         x=x_axis,
#         y="aqi",
#         color=x_axis if df_box[x_axis].dtype == "object" else None,
#         title=f"AQI distribution by {x_axis}"
#     )
#     st.plotly_chart(fig, use_container_width=True)

# # --------------------------------------------------
# # Pie
# # --------------------------------------------------
# elif chart_type == "Pie":
#     # Merge minimal columns needed
#     df_pie = fact.merge(
#         site[["site_key", "sitename"]],
#         on="site_key",
#         how="left"
#     ).merge(
#         param[["parameter_key", "parameter"]],
#         on="parameter_key",
#         how="left"
#     )

#     # Filter by region if selected
#     if selected_region != "All":
#         region = US_REGIONS[selected_region]
#         df_pie = df_pie.merge(site[["site_key", "latitude", "longitude"]], on="site_key")
#         df_pie = df_pie[
#             (df_pie["latitude"] >= region["lat_min"]) &
#             (df_pie["latitude"] <= region["lat_max"]) &
#             (df_pie["longitude"] >= region["lon_min"]) &
#             (df_pie["longitude"] <= region["lon_max"])
#         ]

#     # Filter by parameter
#     if selected_parameter != "All":
#         df_pie = df_pie[df_pie["parameter"] == selected_parameter]

#     # Ensure x_axis exists in df_pie
#     if x_axis not in df_pie.columns:
#         st.warning(f"Column {x_axis} not found in data. Using 'parameter' instead.")
#         x_axis = "parameter"

#     # Aggregate counts
#     counts = df_pie[x_axis].value_counts().reset_index()
#     counts.columns = [x_axis, "count"]

#     fig = px.pie(
#         counts,
#         names=x_axis,
#         values="count",
#         title=f"Distribution of {x_axis}"
#     )
#     st.plotly_chart(fig, use_container_width=True)

# # --------------------------------------------------
# # Pollution Map
# # --------------------------------------------------
# elif chart_type == "Pollution Map":
#     if selected_parameter != "All":
#         allowed_keys = param[param["parameter"] == selected_parameter]["parameter_key"]
#         fact_filtered = fact[fact["parameter_key"].isin(allowed_keys)]
#     else:
#         fact_filtered = fact
#     df_map = fact_filtered.merge(site[["site_key", "sitename", "latitude", "longitude"]], on="site_key")
#     df_map = df_map.merge(date[["date_key", "date", "hour"]], on="date_key")
#     # with st.expander("show df_map"):
#     #     st.dataframe(df_map)
    
#     min_date = df_map["date_y"].min()
#     max_date = df_map["date_y"].max()
#     selected_date = st.sidebar.date_input(
#         "Date",
#         value=min_date,
#         min_value=min_date,
#         max_value=max_date
#     )

#     selected_hour = st.sidebar.slider("Hour (UTC)", 0, 23, 0)

#     df_map = df_map[
#         (df_map["date_y"] == selected_date) &
#         (df_map["hour_y"] == selected_hour)
#     ].groupby(
#         ["sitename", "latitude", "longitude"],
#         as_index=False
#     )["aqi"].mean()


#     # Filter by region
#     if selected_region != "All":
#         region = US_REGIONS[selected_region]
#         df_line = df_map[
#             (df_map["latitude"] >= region["lat_min"]) &
#             (df_map["latitude"] <= region["lat_max"]) &
#             (df_map["longitude"] >= region["lon_min"]) &
#             (df_map["longitude"] <= region["lon_max"])
#         ]

#     aqi_colorscale = [
#         # Good (0–50)
#         [0.00, "green"],
#         [0.10, "green"],

#         # Moderate (51–100)
#         [0.10, "yellow"],
#         [0.20, "yellow"],

#         # Unhealthy for Sensitive Groups (101–150)
#         [0.20, "orange"],
#         [0.30, "orange"],

#         # Unhealthy (151–200)
#         [0.30, "red"],
#         [0.40, "red"],

#         # Very Unhealthy (201–300)
#         [0.40, "purple"],
#         [0.60, "purple"],

#         # Hazardous (301–higher)
#         [0.60, "maroon"],
#         [1.00, "maroon"]
#     ]

#     fig = px.scatter_mapbox(
#         df_map,
#         lat="latitude",
#         lon="longitude",
#         color="aqi",
#         size="aqi",
#         zoom=4,
#         hover_name="sitename",
#         color_continuous_scale=aqi_colorscale
#     )

#     fig.update_layout(
#         coloraxis=dict(
#             cmin=0,
#             cmax=500,
#             colorbar=dict(
#                 title="AQI",
#                 tickvals=[0, 51, 101, 151, 201, 301, 500],
#                 ticktext=[
#                     "0 Good",
#                     "51 Moderate",
#                     "101 Unhealthy for Sensitive Groups",
#                     "151 Unhealthy",
#                     "201 Very Unhealthy",
#                     "301 Hazardous",
#                     "500 Hazardous"
#                 ]
#             )
#         )
#     )

#     fig.update_layout(mapbox_style="open-street-map")
#     fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
#     st.plotly_chart(fig, use_container_width=True)


# # --------------------------------------------------
# # Data preview
# # --------------------------------------------------
# with st.expander("Show fact data"):
#     st.dataframe(fact)
# with st.expander("Show site data"):
#     st.dataframe(site)
# with st.expander("Show param data"):
#     st.dataframe(param)
# with st.expander("Show date data"):
#     st.dataframe(date)
# with st.expander("Show category data"):
#     st.dataframe(category)