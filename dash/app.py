# -*- coding: utf-8 -*-
# mypy: ignore-errors
import os

import flask
import pandas as pd
import plotly.express as px
import snowflake.connector
from dash import Dash, Input, Output, callback, dash_table, dcc, html
from posit import connect

QUERY = "SELECT * FROM SALES"

cached_df = None

app = Dash(__name__)

app.layout = html.Div(
    [
        html.H1("Snowflake Sales Dashboard", style={"textAlign": "center"}),
        html.Div(id="greeting", style={"textAlign": "center", "marginBottom": "10px"}),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Category"),
                        dcc.Dropdown(id="category-filter", value="All"),
                    ],
                    style={
                        "width": "30%",
                        "display": "inline-block",
                        "padding": "10px",
                    },
                ),
                html.Div(
                    [
                        html.Label("Region"),
                        dcc.Dropdown(id="region-filter", value="All"),
                    ],
                    style={
                        "width": "30%",
                        "display": "inline-block",
                        "padding": "10px",
                    },
                ),
            ],
            style={"textAlign": "center"},
        ),
        html.Div(
            id="value-boxes",
            style={
                "display": "flex",
                "justifyContent": "center",
                "gap": "20px",
                "margin": "20px 0",
            },
        ),
        html.Div(
            [
                html.Div(
                    dcc.Graph(id="chart-category"),
                    style={"width": "50%", "display": "inline-block"},
                ),
                html.Div(
                    dcc.Graph(id="chart-region"),
                    style={"width": "50%", "display": "inline-block"},
                ),
            ]
        ),
        html.Div(
            dcc.Graph(id="chart-trend"),
        ),
        html.H3("Sales Data", style={"marginTop": "20px"}),
        html.Div(id="sales-table"),
        html.Div(id="dummy"),
    ]
)


def value_box(title, value, color):
    return html.Div(
        [
            html.Div(title, style={"fontSize": "0.85rem", "color": "#6c757d"}),
            html.Div(value, style={"fontSize": "1.5rem", "fontWeight": "bold"}),
        ],
        style={
            "backgroundColor": color,
            "color": "white",
            "padding": "15px 25px",
            "borderRadius": "8px",
            "textAlign": "center",
            "minWidth": "150px",
        },
    )


def fetch_data(session_token):
    global cached_df
    if cached_df is not None:
        return cached_df

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
    cached_df = df
    return df


def get_filtered(df, category, region):
    if df is None or df.empty:
        return pd.DataFrame()
    if category and category != "All":
        df = df[df["CATEGORY"] == category]
    if region and region != "All":
        df = df[df["REGION"] == region]
    return df


@callback(
    Output("greeting", "children"),
    Output("category-filter", "options"),
    Output("region-filter", "options"),
    Output("value-boxes", "children"),
    Output("chart-category", "figure"),
    Output("chart-region", "figure"),
    Output("chart-trend", "figure"),
    Output("sales-table", "children"),
    Input("dummy", "children"),
    Input("category-filter", "value"),
    Input("region-filter", "value"),
)
def update_page(_, category, region):
    session_token = flask.request.headers.get("Posit-Connect-User-Session-Token")

    try:
        df = fetch_data(session_token)
    except Exception as e:
        empty = px.bar(title="No data")
        return (
            f"Error: {e}",
            [],
            [],
            [value_box("Error", "--", "#dc3545")] * 3,
            empty,
            empty,
            empty,
            html.Div(),
        )

    categories = ["All"] + sorted(df["CATEGORY"].dropna().unique().tolist())
    regions = ["All"] + sorted(df["REGION"].dropna().unique().tolist())
    category_opts = [{"label": c, "value": c} for c in categories]
    region_opts = [{"label": r, "value": r} for r in regions]

    filtered = get_filtered(df, category, region)

    # Value boxes
    if filtered.empty:
        boxes = [
            value_box("Total Sales", "--", "#0d6efd"),
            value_box("Total Orders", "--", "#0dcaf0"),
            value_box("Avg Order Value", "--", "#198754"),
        ]
    else:
        boxes = [
            value_box(
                "Total Sales",
                f"${filtered['TOTAL_AMOUNT'].sum():,.2f}",
                "#0d6efd",
            ),
            value_box("Total Orders", f"{len(filtered):,}", "#0dcaf0"),
            value_box(
                "Avg Order Value",
                f"${filtered['TOTAL_AMOUNT'].mean():,.2f}",
                "#198754",
            ),
        ]

    # Category chart
    if filtered.empty:
        fig_category = px.bar(title="Sales by Category")
    else:
        agg = filtered.groupby("CATEGORY", as_index=False)["TOTAL_AMOUNT"].sum()
        fig_category = px.bar(
            agg,
            x="CATEGORY",
            y="TOTAL_AMOUNT",
            color="CATEGORY",
            title="Sales by Category",
            labels={"TOTAL_AMOUNT": "Total Sales ($)", "CATEGORY": "Category"},
        )

    # Region chart
    if filtered.empty:
        fig_region = px.pie(title="Sales by Region")
    else:
        agg = filtered.groupby("REGION", as_index=False)["TOTAL_AMOUNT"].sum()
        fig_region = px.pie(
            agg,
            names="REGION",
            values="TOTAL_AMOUNT",
            title="Sales by Region",
            labels={"TOTAL_AMOUNT": "Total Sales ($)", "REGION": "Region"},
        )

    # Trend chart
    if filtered.empty:
        fig_trend = px.line(title="Monthly Sales Trend")
    else:
        agg = (
            filtered.groupby("MONTH", as_index=False)["TOTAL_AMOUNT"]
            .sum()
            .sort_values("MONTH")
        )
        fig_trend = px.line(
            agg,
            x="MONTH",
            y="TOTAL_AMOUNT",
            markers=True,
            title="Monthly Sales Trend",
            labels={"TOTAL_AMOUNT": "Total Sales ($)", "MONTH": "Month"},
        )

    # Data table
    if filtered.empty:
        table = html.Div("No data")
    else:
        display = filtered[
            ["SALE_DATE", "PRODUCT_NAME", "CATEGORY", "QUANTITY", "UNIT_PRICE", "TOTAL_AMOUNT", "REGION", "CUSTOMER_NAME"]
        ].copy()
        display["SALE_DATE"] = display["SALE_DATE"].dt.strftime("%Y-%m-%d")
        table = dash_table.DataTable(
            data=display.to_dict("records"),
            columns=[{"name": c, "id": c} for c in display.columns],
            page_size=15,
            filter_action="native",
            sort_action="native",
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "left", "padding": "8px"},
            style_header={"fontWeight": "bold"},
        )

    return (
        f"Loaded {len(df)} rows from Snowflake",
        category_opts,
        region_opts,
        boxes,
        fig_category,
        fig_region,
        fig_trend,
        table,
    )


if __name__ == "__main__":
    app.run(debug=True)
