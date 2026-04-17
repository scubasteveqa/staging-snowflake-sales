import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from posit import connect
from shiny import App, Inputs, Outputs, Session, reactive, render, ui
from shinywidgets import output_widget, render_widget

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.input_action_button("load_data", "Refresh Data", class_="btn-primary w-100"),
        ui.tags.script("setTimeout(function() { document.getElementById('load_data').click(); }, 500);"),
        ui.input_select(
            "category",
            "Category",
            choices=["All", "Electronics", "Furniture"],
            selected="All",
        ),
        ui.input_select(
            "region",
            "Region",
            choices=["All", "North", "South", "East", "West"],
            selected="All",
        ),
        width=250,
    ),
    ui.layout_columns(
        ui.value_box("Total Sales", ui.output_text("total_sales"), theme="primary"),
        ui.value_box("Total Orders", ui.output_text("total_orders"), theme="info"),
        ui.value_box("Avg Order Value", ui.output_text("avg_order"), theme="success"),
        col_widths=[4, 4, 4],
    ),
    ui.layout_columns(
        ui.card(
            ui.card_header("Sales by Category"),
            output_widget("chart_category"),
        ),
        ui.card(
            ui.card_header("Sales by Region"),
            output_widget("chart_region"),
        ),
        col_widths=[6, 6],
    ),
    ui.layout_columns(
        ui.card(
            ui.card_header("Monthly Sales Trend"),
            output_widget("chart_trend"),
        ),
        col_widths=[12],
    ),
    ui.card(
        ui.card_header("Sales Data"),
        ui.output_data_frame("sales_table"),
    ),
    title="Snowflake Sales Dashboard",
)


def server(i: Inputs, o: Outputs, session: Session):
    data = reactive.Value(None)

    @reactive.effect
    @reactive.event(i.load_data)
    def fetch_data():
        try:
            session_token = session.http_conn.headers.get(
                "Posit-Connect-User-Session-Token"
            )
            if not session_token:
                ui.notification_show(
                    "No session token found. Deploy this app on Posit Connect.",
                    type="error",
                )
                return

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
                df = pd.read_sql("SELECT * FROM SALES", conn)
            finally:
                conn.close()

            df.columns = df.columns.str.upper()
            df["SALE_DATE"] = pd.to_datetime(df["SALE_DATE"])
            df["MONTH"] = df["SALE_DATE"].dt.to_period("M").astype(str)
            data.set(df)
            ui.notification_show(
                f"Loaded {len(df)} rows from Snowflake", type="message"
            )
        except Exception as e:
            ui.notification_show(
                f"Error: {type(e).__name__}: {e}", type="error", duration=10
            )

    @reactive.calc
    def filtered_data():
        df = data()
        if df is None:
            return None
        if i.category() != "All":
            df = df[df["CATEGORY"] == i.category()]
        if i.region() != "All":
            df = df[df["REGION"] == i.region()]
        return df

    @render.text
    def total_sales():
        df = filtered_data()
        if df is None:
            return "--"
        return f"${df['TOTAL_AMOUNT'].sum():,.2f}"

    @render.text
    def total_orders():
        df = filtered_data()
        if df is None:
            return "--"
        return str(len(df))

    @render.text
    def avg_order():
        df = filtered_data()
        if df is None or len(df) == 0:
            return "--"
        return f"${df['TOTAL_AMOUNT'].mean():,.2f}"

    @render_widget
    def chart_category():
        df = filtered_data()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading data from Snowflake...", template="plotly_white", height=400)
            return fig
        agg = df.groupby("CATEGORY", as_index=False)["TOTAL_AMOUNT"].sum()
        return px.bar(
            agg,
            x="CATEGORY",
            y="TOTAL_AMOUNT",
            color="CATEGORY",
            labels={"TOTAL_AMOUNT": "Total Sales ($)", "CATEGORY": "Category"},
        )

    @render_widget
    def chart_region():
        df = filtered_data()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading data from Snowflake...", template="plotly_white", height=400)
            return fig
        agg = df.groupby("REGION", as_index=False)["TOTAL_AMOUNT"].sum()
        return px.pie(
            agg,
            names="REGION",
            values="TOTAL_AMOUNT",
            labels={"TOTAL_AMOUNT": "Total Sales ($)", "REGION": "Region"},
        )

    @render_widget
    def chart_trend():
        df = filtered_data()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading data from Snowflake...", template="plotly_white", height=400)
            return fig
        agg = df.groupby("MONTH", as_index=False)["TOTAL_AMOUNT"].sum()
        return px.line(
            agg,
            x="MONTH",
            y="TOTAL_AMOUNT",
            markers=True,
            labels={"TOTAL_AMOUNT": "Total Sales ($)", "MONTH": "Month"},
        )

    @render.data_frame
    def sales_table():
        df = filtered_data()
        if df is None:
            return render.DataGrid(pd.DataFrame())
        display = df[
            ["SALE_DATE", "PRODUCT_NAME", "CATEGORY", "QUANTITY", "UNIT_PRICE", "TOTAL_AMOUNT", "REGION", "CUSTOMER_NAME"]
        ].copy()
        display["SALE_DATE"] = display["SALE_DATE"].dt.strftime("%Y-%m-%d")
        return render.DataGrid(display, filters=True)


app = App(app_ui, server)
