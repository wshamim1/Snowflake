import os
from typing import Optional, Tuple

import altair as alt
import pandas as pd
import snowflake.connector
import streamlit as st


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


@st.cache_resource
def get_connection() -> Tuple[str, object, Optional[str]]:
    """
    Returns a tuple of (mode, handle, error).
    mode: 'session' for Snowflake Streamlit, 'connector' for local/.env.
    """
    try:
        from snowflake.snowpark.context import get_active_session

        session = get_active_session()
        return "session", session, None
    except Exception:
        pass

    try:
        from dotenv import load_dotenv

        load_dotenv()
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        return "connector", conn, None
    except Exception as e:
        return "none", None, str(e)


def run_select(mode: str, handle: object, query: str) -> Optional[pd.DataFrame]:
    try:
        if mode == "session":
            return handle.sql(query).to_pandas()

        cursor = handle.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description] if cursor.description else []
            return pd.DataFrame(rows, columns=cols)
        finally:
            cursor.close()
    except Exception as e:
        st.error(f"Query failed: {e}")
        return None


def run_dml(mode: str, handle: object, query: str) -> bool:
    try:
        if mode == "session":
            handle.sql(query).collect()
            return True

        cursor = handle.cursor()
        try:
            cursor.execute(query)
            handle.commit()
            return True
        finally:
            cursor.close()
    except Exception as e:
        st.error(f"Operation failed: {e}")
        return False


def table_exists(mode: str, handle: object, table_name: str) -> bool:
    """Check if a table exists in the current schema."""
    try:
        query = f"SELECT 1 FROM {table_name} LIMIT 1;"
        if mode == "session":
            handle.sql(query).collect()
        else:
            cursor = handle.cursor()
            try:
                cursor.execute(query)
            finally:
                cursor.close()
        return True
    except Exception:
        return False


def seed_sales_demo_data(mode: str, handle: object, sales_table: str) -> bool:
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {sales_table} (
        ORDER_ID INT,
        ORDER_DATE DATE,
        REGION STRING,
        PRODUCT STRING,
        CATEGORY STRING,
        QUANTITY INT,
        SALES_AMOUNT NUMBER(10,2)
    );
    """

    delete_sql = f"DELETE FROM {sales_table};"

    insert_sql = f"""
    INSERT INTO {sales_table} (ORDER_ID, ORDER_DATE, REGION, PRODUCT, CATEGORY, QUANTITY, SALES_AMOUNT)
    VALUES
    (1001, '2026-01-05', 'North', 'Laptop X', 'Electronics', 2, 2400.00),
    (1002, '2026-01-08', 'South', 'Phone A', 'Electronics', 5, 3000.00),
    (1003, '2026-01-10', 'East', 'Desk Pro', 'Furniture', 3, 1350.00),
    (1004, '2026-01-12', 'West', 'Chair Plus', 'Furniture', 6, 1800.00),
    (1005, '2026-01-18', 'North', 'Pen Set', 'Stationery', 20, 400.00),
    (1006, '2026-02-02', 'South', 'Notebook Max', 'Stationery', 25, 625.00),
    (1007, '2026-02-11', 'East', 'Monitor 24', 'Electronics', 4, 1600.00),
    (1008, '2026-02-16', 'West', 'Cabinet 2D', 'Furniture', 2, 900.00),
    (1009, '2026-02-22', 'North', 'Mouse Z', 'Electronics', 10, 500.00),
    (1010, '2026-03-01', 'South', 'Paper Ream', 'Stationery', 30, 450.00),
    (1011, '2026-03-04', 'East', 'Table Lite', 'Furniture', 2, 1100.00),
    (1012, '2026-03-05', 'West', 'Headset Q', 'Electronics', 7, 980.00);
    """

    return (
        run_dml(mode, handle, create_sql)
        and run_dml(mode, handle, delete_sql)
        and run_dml(mode, handle, insert_sql)
    )


st.set_page_config(page_title="Snowflake Explorer", layout="wide")
st.title("Snowflake Explorer")

mode, handle, conn_error = get_connection()

if conn_error:
    st.error(f"Connection error: {conn_error}")
    st.stop()

st.success(f"Connected in `{mode}` mode")

sales_table = st.sidebar.text_input("Sales table name", value="SALES_DEMO").strip() or "SALES_DEMO"

if st.sidebar.button("Create / Refresh Sales Demo Data"):
    if seed_sales_demo_data(mode, handle, sales_table):
        st.sidebar.success(f"Sales demo data loaded into {sales_table}.")

tab_query, tab_create, tab_crud, tab_chart = st.tabs(["SQL Runner", "Create Table", "Insert / Update / Delete", "Bar Chart"])

with tab_create:
    st.subheader("Create Sales Demo Table")
    st.write("This table stores sales order data with the following columns:")
    
    schema_info = {
        "ORDER_ID": "INT - Unique order identifier",
        "ORDER_DATE": "DATE - Date the order was placed",
        "REGION": "STRING - Geographic region (North, South, East, West)",
        "PRODUCT": "STRING - Product name",
        "CATEGORY": "STRING - Product category (Electronics, Furniture, Stationery)",
        "QUANTITY": "INT - Number of items ordered",
        "SALES_AMOUNT": "NUMBER(10,2) - Total sales amount in dollars"
    }
    
    for col, desc in schema_info.items():
        st.write(f"- **{col}**: {desc}")
    
    st.divider()
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Create / Refresh Table with Sample Data", use_container_width=True):
            with st.spinner("Creating table and loading sample data..."):
                if seed_sales_demo_data(mode, handle, sales_table):
                    st.success(f"✅ Table '{sales_table}' created and populated with 12 sample records.")
                    st.balloons()
                else:
                    st.error("Failed to create/refresh table.")
    
    with col2:
        if st.button("📋 View Current Table", use_container_width=True):
            if table_exists(mode, handle, sales_table):
                df = run_select(mode, handle, f"SELECT * FROM {sales_table} ORDER BY ORDER_ID;")
                if df is not None:
                    st.success(f"Showing {len(df)} records from '{sales_table}'")
                    st.dataframe(df, use_container_width=True)
            else:
                st.warning(f"Table '{sales_table}' does not exist yet.")

with tab_query:
    query = st.text_area("SQL Query", value=f"SELECT * FROM {sales_table} LIMIT 20;", height=140)
    if st.button("Run Query"):
        if not query.strip():
            st.warning("Please provide a SQL query.")
        else:
            df = run_select(mode, handle, query)
            if df is not None:
                st.success(f"Returned {len(df)} row(s).")
                st.dataframe(df, use_container_width=True)

with tab_crud:
    if not table_exists(mode, handle, sales_table):
        st.warning(f"⚠️ Table '{sales_table}' does not exist. Click 'Create / Refresh Sales Demo Data' in the sidebar first.")
    else:
        st.subheader("Insert sales record")
        c1, c2 = st.columns(2)
        with c1:
            ins_order_id = st.number_input("Order ID", min_value=1, value=2001, step=1)
        with c2:
            ins_date = st.date_input("Order Date")
        
        c3, c4 = st.columns(2)
        with c3:
            ins_region = st.selectbox("Region", ["North", "South", "East", "West"])
        with c4:
            ins_product = st.text_input("Product", value="New Product")
        
        c5, c6 = st.columns(2)
        with c5:
            ins_category = st.selectbox("Category", ["Electronics", "Furniture", "Stationery"])
        with c6:
            ins_quantity = st.number_input("Quantity", min_value=1, value=1, step=1)
        
        ins_amount = st.number_input("Sales Amount", min_value=0.0, value=100.0, step=0.01)

        if st.button("Insert Record"):
            query = (
                f"INSERT INTO {sales_table} (ORDER_ID, ORDER_DATE, REGION, PRODUCT, CATEGORY, QUANTITY, SALES_AMOUNT) "
                f"VALUES ({int(ins_order_id)}, '{ins_date}', '{ins_region}', '{_escape_sql(ins_product)}', '{ins_category}', {int(ins_quantity)}, {float(ins_amount)});"
            )
            if run_dml(mode, handle, query):
                st.success("Record inserted successfully.")

        st.subheader("Update sales record")
        upd_order_id = st.number_input("Order ID to update", min_value=1, value=1001, step=1)
        upd_amount = st.number_input("New Sales Amount", min_value=0.0, value=500.0, step=0.01)

        if st.button("Update Record"):
            query = (
                f"UPDATE {sales_table} SET SALES_AMOUNT = {float(upd_amount)} "
                f"WHERE ORDER_ID = {int(upd_order_id)};"
            )
            if run_dml(mode, handle, query):
                st.success("Record updated successfully.")

        st.subheader("Delete sales record")
        del_order_id = st.number_input("Order ID to delete", min_value=1, value=1001, step=1)
        if st.button("Delete Record"):
            query = f"DELETE FROM {sales_table} WHERE ORDER_ID = {int(del_order_id)};"
            if run_dml(mode, handle, query):
                st.success("Record deleted successfully.")

with tab_chart:
    st.subheader("Sales chart")
    chart_type = st.selectbox("Chart type", ["Bar", "Pie"], index=0)
    group_by = st.selectbox("Group by", ["CATEGORY", "REGION", "PRODUCT"], index=0)
    metric = st.selectbox("Metric", ["SALES_AMOUNT", "QUANTITY"], index=0)

    agg_expr = "SUM(SALES_AMOUNT)" if metric == "SALES_AMOUNT" else "SUM(QUANTITY)"
    alias = "TOTAL_VALUE"

    chart_query = (
        f"SELECT {group_by}, {agg_expr} AS {alias} FROM {sales_table} "
        f"GROUP BY {group_by} ORDER BY {alias} DESC"
    )

    df = run_select(mode, handle, chart_query)
    if df is not None and not df.empty:
        st.dataframe(df, use_container_width=True)

        if chart_type == "Bar":
            st.bar_chart(df.set_index(group_by)[alias])
        else:
            pie = (
                alt.Chart(df)
                .mark_arc()
                .encode(
                    theta=alt.Theta(field=alias, type="quantitative"),
                    color=alt.Color(field=group_by, type="nominal"),
                    tooltip=[group_by, alias],
                )
            )
            st.altair_chart(pie, use_container_width=True)
    else:
        st.info(
            "No sales data found. Click 'Create / Refresh Sales Demo Data' in the sidebar first."
        )
