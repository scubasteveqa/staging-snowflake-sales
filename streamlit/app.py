# -*- coding: utf-8 -*-
# mypy: ignore-errors
import os

import pandas as pd
import plotly.express as px
import snowflake.connector
import streamlit as st
from posit import connect

QUERY = "SELECT * FROM SALES"

st.set_page_config(page_title="Snowflake Sales Dashboard", layout="wide")
st.title("Snowflake Sales Dashboard")


@st.cache_data(ttl=300)
def load_data(session_token):
    client = connect.Client()
    credentials = client.oauth.get_credentials(session_token)
    access_token = credentials["access_token"]

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        token=access_token,
        authenticator="oauth",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "DEFAULT_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "STEVEW_TEST_DB"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )
    try:
        df = pd.read_sql(QUERY, conn)
    finally:
        conn.close()

    df.columns = df.columns.str.upper()
    df["SALE_DATE"] = pd.to_datetime(df["SALE_DATE"])
    df["MONTH"] = df["SALE_DATE"].dt.to_period("M").astype(str)
    return df


session_token = st.context.headers.get("Posit-Connect-User-Session-Token")

try:
    df = load_data(session_token)
    st.success(f"Loaded {len(df)} rows from Snowflake")
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    categories = ["All"] + sorted(df["CATEGORY"].dropna().unique().tolist())
    category = st.selectbox("Category", categories)

    regions = ["All"] + sorted(df["REGION"].dropna().unique().tolist())
    region = st.selectbox("Region", regions)

# Apply filters
filtered = df.copy()
if category != "All":
    filtered = filtered[filtered["CATEGORY"] == category]
if region != "All":
    filtered = filtered[filtered["REGION"] == region]

# Value boxes
col1, col2, col3 = st.columns(3)
col1.metric("Total Sales", f"${filtered['TOTAL_AMOUNT'].sum():,.2f}")
col2.metric("Total Orders", f"{len(filtered):,}")
col3.metric(
    "Avg Order Value",
    f"${filtered['TOTAL_AMOUNT'].mean():,.2f}" if len(filtered) > 0 else "--",
)

# Charts row 1
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Sales by Category")
    agg = filtered.groupby("CATEGORY", as_index=False)["TOTAL_AMOUNT"].sum()
    fig = px.bar(
        agg,
        x="CATEGORY",
        y="TOTAL_AMOUNT",
        color="CATEGORY",
        labels={"TOTAL_AMOUNT": "Total Sales ($)", "CATEGORY": "Category"},
    )
    st.plotly_chart(fig, use_container_width=True)

with chart_col2:
    st.subheader("Sales by Region")
    agg = filtered.groupby("REGION", as_index=False)["TOTAL_AMOUNT"].sum()
    fig = px.pie(
        agg,
        names="REGION",
        values="TOTAL_AMOUNT",
        labels={"TOTAL_AMOUNT": "Total Sales ($)", "REGION": "Region"},
    )
    st.plotly_chart(fig, use_container_width=True)

# Monthly trend
st.subheader("Monthly Sales Trend")
agg = (
    filtered.groupby("MONTH", as_index=False)["TOTAL_AMOUNT"]
    .sum()
    .sort_values("MONTH")
)
fig = px.line(
    agg,
    x="MONTH",
    y="TOTAL_AMOUNT",
    markers=True,
    labels={"TOTAL_AMOUNT": "Total Sales ($)", "MONTH": "Month"},
)
st.plotly_chart(fig, use_container_width=True)

# Sales data table
st.subheader("Sales Data")
display = filtered[
    ["SALE_DATE", "PRODUCT_NAME", "CATEGORY", "QUANTITY", "UNIT_PRICE", "TOTAL_AMOUNT", "REGION", "CUSTOMER_NAME"]
].copy()
display["SALE_DATE"] = display["SALE_DATE"].dt.strftime("%Y-%m-%d")
st.dataframe(display, use_container_width=True)
