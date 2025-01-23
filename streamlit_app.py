import os
import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

# Streamlit Page Config
st.set_page_config(page_title="📊 Analytics Dashboard", layout="wide")

# Securely Fetch Credentials from Streamlit Secrets
PROJECT_ID = st.secrets["GCP_PROJECT_ID"]

# Initialize BigQuery Client with OAuth Authentication
client = bigquery.Client(project=PROJECT_ID)

# SQL Query for Data Retrieval
QUERY = """
WITH filtered_events AS (
    SELECT
        FORMAT_DATE('%d-%m-%Y', PARSE_DATE('%Y%m%d', event_date)) AS event_dates,
        event_name,
        event_params
    FROM
        `mg-analytics-441108.analytics_461435207.events_*`
    WHERE
        _TABLE_SUFFIX <= FORMAT_DATE('%Y%m%d', CURRENT_DATE())  -- Fetch all data till today
),
extracted_values AS (
    SELECT
        event_dates,
        event_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'model') AS model_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'variant') AS variant_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'color' AND event_name = 'color_changed') AS color_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'feature' AND event_name = 'feature_clicked') AS feature_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'accessory' AND event_name = 'accessory_clicked') AS accessory_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'pack' AND event_name = 'accessory_pack_clicked') AS pack_name
    FROM filtered_events
),
aggregated_data AS (
    SELECT
        event_dates,
        model_name,
        variant_name,
        color_name,
        feature_name,
        accessory_name,
        pack_name,
        COUNT(*) AS event_count
    FROM extracted_values
    WHERE model_name IS NOT NULL 
        AND variant_name IS NOT NULL 
        AND (color_name IS NOT NULL OR feature_name IS NOT NULL OR accessory_name IS NOT NULL OR pack_name IS NOT NULL)
    GROUP BY event_dates, model_name, variant_name, color_name, feature_name, accessory_name, pack_name
)
SELECT
    event_dates,
    model_name AS distinct_model_names,
    variant_name AS distinct_variant_names,
    color_name AS distinct_color_names,
    feature_name AS distinct_feature_names,
    accessory_name AS distinct_accessory_names,
    pack_name AS distinct_pack_names,
    event_count AS total_event_counts
FROM aggregated_data
ORDER BY event_dates, model_name, variant_name, color_name, feature_name, accessory_name, pack_name;
"""

# Fetch Data
@st.cache_data
def get_data():
    df = client.query(QUERY).to_dataframe()
    df["event_dates"] = pd.to_datetime(df["event_dates"], format="%d-%m-%Y")
    return df

df = get_data()

# Sidebar Filters
st.sidebar.header("Filters")
start_date = st.sidebar.date_input("Start Date", df["event_dates"].min())
end_date = st.sidebar.date_input("End Date", df["event_dates"].max())

model_list = ["All Models"] + sorted(df["distinct_model_names"].dropna().unique().tolist())
selected_model = st.sidebar.selectbox("Select Model", model_list)

# Apply Filters
filtered_df = df[(df["event_dates"] >= pd.to_datetime(start_date)) & (df["event_dates"] <= pd.to_datetime(end_date))]
if selected_model != "All Models":
    filtered_df = filtered_df[filtered_df["distinct_model_names"] == selected_model]

# Dashboard
st.title("📊 Analytics Dashboard")
st.subheader(f"Data from {start_date} to {end_date}")

# Line Chart
fig = px.line(filtered_df, x="event_dates", y="total_event_counts", title="User Interactions Over Time", template="plotly_dark")
st.plotly_chart(fig, use_container_width=True)

# Show Raw Data
st.subheader("Raw Data")
st.dataframe(filtered_df)
