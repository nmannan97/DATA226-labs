# stock_data_dag_yfinance_fixed.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, date
import os
import yfinance as yf
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# Snowflake settings
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "BISON_QUERY_WH")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB", "USER_BISON_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_TABLE = "STOCK_DATA"

# DAG default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "stock_data_to_snowflake_yfinance_fixed",
    default_args=default_args,
    start_date=datetime(2025, 10, 3),
    schedule="@daily",
    catchup=False,
) as dag:

    @task
    def fetch_stock_data(symbol="MSFT", period="1y"):

        # Download stock data
        df = yf.download(symbol, period=period)

        # Reset index to make the date part of columns
        df = df.reset_index()

        # Flatten MultiIndex columns
        df.columns = ['_'.join(filter(None, map(str, col))) if isinstance(col, tuple) else col for col in df.columns]

        # Rename columns consistently
        rename_map = {
            "Date_": "date",
            f"Open_{symbol}": "open",
            f"High_{symbol}": "high",
            f"Low_{symbol}": "low",
            f"Close_{symbol}": "close",
            f"Adj Close_{symbol}": "adj_close",
            f"Volume_{symbol}": "volume"
        }
        df.rename(columns=rename_map, inplace=True)

        # Ensure 'date' is datetime, then convert to string
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        else:
            raise ValueError(f"'date' column not found. Columns: {df.columns.tolist()}")

        # Convert numeric columns safely
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Return list of dicts for XCom
        return df.to_dict(orient="records")

    @task
    def transform_data(data: list):
        """
        Ensure numeric types are correct for Snowflake insertion.
        """
        for row in data:
            row["open"] = float(row["open"])
            row["high"] = float(row["high"])
            row["low"] = float(row["low"])
            row["close"] = float(row["close"])
            row["volume"] = int(row["volume"])
        return data

    @task
    def load_to_snowflake(data: list):
        """
        Load transformed stock data into Snowflake.
        """
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        df = pd.DataFrame(data)
        with hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
            cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            # Optionally clear the table first
            cur.execute(f"TRUNCATE TABLE IF EXISTS {SNOWFLAKE_TABLE}")
            # Bulk insert
            write_pandas(conn, df, table_name=SNOWFLAKE_TABLE, schema=SNOWFLAKE_SCHEMA, database=SNOWFLAKE_DB)

    # DAG task dependencies
    raw_data = fetch_stock_data()
    transformed_data = transform_data(raw_data)
    load_to_snowflake(transformed_data)
