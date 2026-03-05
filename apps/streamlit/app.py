import os
from typing import Optional, Tuple

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


st.set_page_config(page_title="Snowflake Explorer", layout="wide")
st.title("Snowflake Explorer")

mode, handle, conn_error = get_connection()

if conn_error:
    st.error(f"Connection error: {conn_error}")
    st.stop()

st.success(f"Connected in `{mode}` mode")

default_table = os.getenv("SNOWFLAKE_TABLE", "TABLE1")
table_name = st.sidebar.text_input("Target table", value=default_table).strip() or "TABLE1"

tab_query, tab_crud, tab_chart = st.tabs(["SQL Runner", "Insert / Update / Delete", "Bar Chart"])

with tab_query:
    query = st.text_area("SQL Query", value=f"SELECT * FROM {table_name} LIMIT 20;", height=140)
    if st.button("Run Query"):
        if not query.strip():
            st.warning("Please provide a SQL query.")
        else:
            df = run_select(mode, handle, query)
            if df is not None:
                st.success(f"Returned {len(df)} row(s).")
                st.dataframe(df, use_container_width=True)

with tab_crud:
    st.subheader("Insert sample row")
    c1, c2 = st.columns(2)
    with c1:
        ins_id = st.number_input("ID", min_value=1, value=1001, step=1)
    with c2:
        ins_name = st.text_input("NAME", value="Streamlit Demo")

    if st.button("Insert"):
        query = (
            f"INSERT INTO {table_name} (ID, NAME, CREATED_AT) "
            f"VALUES ({int(ins_id)}, '{_escape_sql(ins_name)}', CURRENT_TIMESTAMP());"
        )
        if not run_dml(mode, handle, query):
            fallback = (
                f"INSERT INTO {table_name} (ID, NAME) "
                f"VALUES ({int(ins_id)}, '{_escape_sql(ins_name)}');"
            )
            if run_dml(mode, handle, fallback):
                st.success("Insert successful (without CREATED_AT column).")
        else:
            st.success("Insert successful.")

    st.subheader("Update row")
    c3, c4 = st.columns(2)
    with c3:
        upd_id = st.number_input("Update ID", min_value=1, value=1001, step=1)
    with c4:
        upd_name = st.text_input("New NAME", value="Updated from Streamlit")

    if st.button("Update"):
        query = (
            f"UPDATE {table_name} SET NAME = '{_escape_sql(upd_name)}' "
            f"WHERE ID = {int(upd_id)};"
        )
        if run_dml(mode, handle, query):
            st.success("Update successful.")

    st.subheader("Delete row")
    del_id = st.number_input("Delete ID", min_value=1, value=1001, step=1)
    if st.button("Delete"):
        query = f"DELETE FROM {table_name} WHERE ID = {int(del_id)};"
        if run_dml(mode, handle, query):
            st.success("Delete successful.")

with tab_chart:
    st.subheader("Rows by NAME")
    chart_query = (
        f"SELECT NAME, COUNT(*) AS CNT FROM {table_name} "
        "GROUP BY NAME ORDER BY CNT DESC"
    )
    df = run_select(mode, handle, chart_query)
    if df is not None and not df.empty:
        st.dataframe(df, use_container_width=True)
        if "NAME" in df.columns and "CNT" in df.columns:
            st.bar_chart(df.set_index("NAME")["CNT"])
    else:
        st.info("No data available for chart.")
