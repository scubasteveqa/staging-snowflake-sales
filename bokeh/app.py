# -*- coding: utf-8 -*-
# mypy: ignore-errors
import os
import math

import pandas as pd
import snowflake.connector
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Select, Div, DataTable, TableColumn, StringFormatter
from bokeh.palettes import Category10
from bokeh.plotting import curdoc, figure
from bokeh.transform import cumsum
from posit import connect

QUERY = "SELECT * FROM SALES"

# Get session token from request headers
request = curdoc().session_context.request
session_token = request.headers.get("Posit-Connect-User-Session-Token", "")

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
    raw_df = pd.read_sql(QUERY, conn)
finally:
    conn.close()

raw_df.columns = raw_df.columns.str.upper()
raw_df["SALE_DATE"] = pd.to_datetime(raw_df["SALE_DATE"])
raw_df["MONTH"] = raw_df["SALE_DATE"].dt.to_period("M").astype(str)
raw_df["SALE_DATE_STR"] = raw_df["SALE_DATE"].dt.strftime("%Y-%m-%d")

# Filters
categories = ["All"] + sorted(raw_df["CATEGORY"].dropna().unique().tolist())
regions = ["All"] + sorted(raw_df["REGION"].dropna().unique().tolist())

category_select = Select(title="Category", value="All", options=categories)
region_select = Select(title="Region", value="All", options=regions)

# Value box displays
sales_div = Div(text="", styles={"font-size": "18px", "font-weight": "bold"})
orders_div = Div(text="", styles={"font-size": "18px", "font-weight": "bold"})
avg_div = Div(text="", styles={"font-size": "18px", "font-weight": "bold"})

# Data sources
category_source = ColumnDataSource(data=dict(CATEGORY=[], TOTAL_AMOUNT=[]))
region_source = ColumnDataSource(
    data=dict(REGION=[], TOTAL_AMOUNT=[], angle=[], color=[])
)
trend_source = ColumnDataSource(data=dict(MONTH=[], TOTAL_AMOUNT=[]))
table_source = ColumnDataSource(data=dict(
    SALE_DATE_STR=[], PRODUCT_NAME=[], CATEGORY=[], QUANTITY=[],
    UNIT_PRICE=[], TOTAL_AMOUNT=[], REGION=[], CUSTOMER_NAME=[],
))

# Category bar chart
category_fig = figure(
    x_range=[],
    title="Sales by Category",
    y_axis_label="Total Sales ($)",
    height=350,
    width=500,
)
category_fig.vbar(x="CATEGORY", top="TOTAL_AMOUNT", width=0.7, source=category_source)
category_fig.xaxis.major_label_orientation = 0.8

# Region pie chart
region_fig = figure(
    title="Sales by Region",
    height=350,
    width=500,
    toolbar_location=None,
    x_range=(-0.5, 1.0),
)
region_fig.wedge(
    x=0,
    y=1,
    radius=0.4,
    start_angle=cumsum("angle", include_zero=True),
    end_angle=cumsum("angle"),
    line_color="white",
    fill_color="color",
    legend_field="REGION",
    source=region_source,
)
region_fig.axis.axis_label = None
region_fig.axis.visible = False
region_fig.grid.grid_line_color = None

# Monthly trend
trend_fig = figure(
    x_range=[],
    title="Monthly Sales Trend",
    y_axis_label="Total Sales ($)",
    height=350,
    width=1020,
)
trend_fig.line(x="MONTH", y="TOTAL_AMOUNT", line_width=2, source=trend_source)
trend_fig.scatter(x="MONTH", y="TOTAL_AMOUNT", size=6, source=trend_source)
trend_fig.xaxis.major_label_orientation = 0.8

# Data table
table_columns = [
    TableColumn(field="SALE_DATE_STR", title="Sale Date", formatter=StringFormatter()),
    TableColumn(field="PRODUCT_NAME", title="Product", formatter=StringFormatter()),
    TableColumn(field="CATEGORY", title="Category", formatter=StringFormatter()),
    TableColumn(field="QUANTITY", title="Quantity"),
    TableColumn(field="UNIT_PRICE", title="Unit Price"),
    TableColumn(field="TOTAL_AMOUNT", title="Total Amount"),
    TableColumn(field="REGION", title="Region", formatter=StringFormatter()),
    TableColumn(field="CUSTOMER_NAME", title="Customer", formatter=StringFormatter()),
]
data_table = DataTable(
    source=table_source, columns=table_columns, width=1020, height=300
)


def update(attr, old, new):
    df = raw_df.copy()
    if category_select.value != "All":
        df = df[df["CATEGORY"] == category_select.value]
    if region_select.value != "All":
        df = df[df["REGION"] == region_select.value]

    # Value boxes
    total_sales = df["TOTAL_AMOUNT"].sum()
    total_orders = len(df)
    avg_order = df["TOTAL_AMOUNT"].mean() if total_orders > 0 else 0

    sales_div.text = f"Total Sales: <b>${total_sales:,.2f}</b>"
    orders_div.text = f"Total Orders: <b>{total_orders:,}</b>"
    avg_div.text = f"Avg Order Value: <b>${avg_order:,.2f}</b>"

    # Category chart
    agg_c = df.groupby("CATEGORY", as_index=False)["TOTAL_AMOUNT"].sum()
    category_source.data = dict(
        CATEGORY=agg_c["CATEGORY"].tolist(),
        TOTAL_AMOUNT=agg_c["TOTAL_AMOUNT"].tolist(),
    )
    category_fig.x_range.factors = agg_c["CATEGORY"].tolist()

    # Region pie chart
    agg_r = df.groupby("REGION", as_index=False)["TOTAL_AMOUNT"].sum()
    total = agg_r["TOTAL_AMOUNT"].sum()
    agg_r["angle"] = agg_r["TOTAL_AMOUNT"] / total * 2 * math.pi if total > 0 else 0
    n_colors = max(len(agg_r), 3)
    palette = Category10[n_colors] if n_colors <= 10 else Category10[10]
    agg_r["color"] = palette[: len(agg_r)]
    region_source.data = dict(
        REGION=agg_r["REGION"].tolist(),
        TOTAL_AMOUNT=agg_r["TOTAL_AMOUNT"].tolist(),
        angle=agg_r["angle"].tolist(),
        color=agg_r["color"].tolist(),
    )

    # Trend chart
    agg_t = (
        df.groupby("MONTH", as_index=False)["TOTAL_AMOUNT"].sum().sort_values("MONTH")
    )
    trend_source.data = dict(
        MONTH=agg_t["MONTH"].tolist(),
        TOTAL_AMOUNT=agg_t["TOTAL_AMOUNT"].tolist(),
    )
    trend_fig.x_range.factors = agg_t["MONTH"].tolist()

    # Data table
    table_source.data = dict(
        SALE_DATE_STR=df["SALE_DATE_STR"].tolist(),
        PRODUCT_NAME=df["PRODUCT_NAME"].tolist(),
        CATEGORY=df["CATEGORY"].tolist(),
        QUANTITY=df["QUANTITY"].tolist(),
        UNIT_PRICE=df["UNIT_PRICE"].tolist(),
        TOTAL_AMOUNT=df["TOTAL_AMOUNT"].tolist(),
        REGION=df["REGION"].tolist(),
        CUSTOMER_NAME=df["CUSTOMER_NAME"].tolist(),
    )


category_select.on_change("value", update)
region_select.on_change("value", update)

# Initial load
update(None, None, None)

# Layout
filters = row(category_select, region_select)
value_boxes = row(sales_div, orders_div, avg_div)
charts_row1 = row(category_fig, region_fig)

layout = column(
    Div(text="<h1>Snowflake Sales Dashboard</h1>"),
    filters,
    value_boxes,
    charts_row1,
    trend_fig,
    Div(text="<h3>Sales Data</h3>"),
    data_table,
)

curdoc().add_root(layout)
curdoc().title = "Snowflake Sales Dashboard"
