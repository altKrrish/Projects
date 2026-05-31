import os
import urllib.parse

import pandas as pd
import streamlit as st

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect


# --------------------------------------------------
# Environment
# --------------------------------------------------
load_dotenv("Secrets.env")

DB_UID = os.getenv("DB_UID")
DB_PWD = os.getenv("DB_PWD")


# --------------------------------------------------
# Database Connection
# --------------------------------------------------
@st.cache_resource
def get_engine():
    try:
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=Connection_String;"  # Your DB Server
            "DATABASE=Database;"         # Your DB Name
            f"UID={DB_UID};"
            f"PWD={DB_PWD}"
        )

        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={params}"
        )

        return engine

    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.stop()


# --------------------------------------------------
# Schema Loader
# --------------------------------------------------
@st.cache_data
def get_database_schema():
    engine = get_engine()

    inspector = inspect(engine)

    schema = {}

    schemas_to_include = [
        "your_schema_name"
    ]

    for idx, schema_name in enumerate(schemas_to_include):

        tables = inspector.get_table_names(schema=schema_name)

        for table_name in tables:

            full_table_name = f"{schema_name}.{table_name}"

            try:
                columns = inspector.get_columns(
                    table_name,
                    schema=schema_name
                )

                schema[full_table_name] = [
                    col["name"]
                    for col in columns
                ]

            except Exception as e:
                print(
                    f"Could not retrieve columns for "
                    f"{full_table_name}: {e}"
                )

        if idx < len(schemas_to_include) - 1:
            schema[f"--- GAP ({schema_name} done) ---"] = []

    return schema


# --------------------------------------------------
# Query Execution
# --------------------------------------------------
def execute_sql(sql_query):
    """
    Execute generated SQL and return dataframe.
    """

    try:
        engine = get_engine()

        df = pd.read_sql(
            sql_query,
            engine
        )

        return df, None

    except Exception as e:
        return None, f"Query failed: {e}"
