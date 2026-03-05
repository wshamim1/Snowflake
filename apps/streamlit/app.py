import streamlit as st
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.operations.select_operations import SelectOperations

st.set_page_config(page_title="Snowflake Explorer", layout="wide")
st.title("Snowflake Query Explorer")

query = st.text_area(
    "SQL Query",
    value="SELECT CURRENT_VERSION();",
    height=140,
)

if st.button("Run Query"):
    if not query.strip():
        st.warning("Please provide a SQL query.")
    else:
        result = SelectOperations.fetch_data(query)
        if result is None:
            st.error("Query failed.")
        else:
            st.success(f"Returned {len(result)} row(s).")
            st.write(result)
