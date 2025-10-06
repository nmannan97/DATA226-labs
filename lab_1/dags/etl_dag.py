# stock_data_dag_yfinance_fixed.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import os
import yfinance as yf
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# Snowflake settings
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB", "USER_DB_BISON")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "BISON_QUERY_WH")
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
        df = yf.download(symbol, period=period)
        df = df.reset_index()
        df.columns = ['_'.join(filter(None, map(str, col))) if isinstance(col, tuple) else col for col in df.columns]

        rename_map = {
            "Date_": "trade_date",   # <-- rename here
            f"Open_{symbol}": "open",
            f"High_{symbol}": "high",
            f"Low_{symbol}": "low",
            f"Close_{symbol}": "close",
            f"Adj Close_{symbol}": "adj_close",
            f"Volume_{symbol}": "volume"
        }
        df.rename(columns=rename_map, inplace=True)

        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.to_dict(orient="records")


    @task
    def transform_data(data: list):
        """
        Ensure numeric types are correct for Snowflake insertion.
        """
        for row in data:
            for col in ["open", "high", "low", "close", "adj_close"]:
                if col in row and row[col] is not None:
                    row[col] = float(row[col])
            if "volume" in row and row["volume"] is not None:
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
            cur.execute(f"TRUNCATE TABLE IF EXISTS {SNOWFLAKE_TABLE}")
            write_pandas(conn, df, table_name=SNOWFLAKE_TABLE, schema=SNOWFLAKE_SCHEMA, database=SNOWFLAKE_DB)

    # DAG task dependencies
    raw_data = fetch_stock_data()
    transformed_data = transform_data(raw_data)
    load_to_snowflake(transformed_data)