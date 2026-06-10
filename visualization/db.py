import pandas as pd
import streamlit as st
from databricks import sql


@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=st.secrets["DATABRICKS_SERVER_HOSTNAME"],
        http_path=st.secrets["DATABRICKS_HTTP_PATH"],
        access_token=st.secrets["DATABRICKS_TOKEN"],
    )


@st.cache_data(ttl=3600)
def run_query(query: str) -> pd.DataFrame:
    with get_connection().cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

    if "sql_log" not in st.session_state:
        st.session_state["sql_log"] = []
    st.session_state["sql_log"].append(query.strip())

    return pd.DataFrame(rows, columns=columns)
