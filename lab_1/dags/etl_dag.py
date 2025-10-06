from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import os
import pandas as pd
import yfinance as yf 

SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "BISON_QUERY_WH")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB", "USER_BISON_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_TABLE = "STOCK_DATA"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "stock_data_to_snowflake_yfinance",
    default_args=default_args,
    start_date=datetime(2025, 10, 3),
    schedule="@daily",
    catchup=False,
) as dag:

    @task
    def fetch_stock_data(symbol="MSFT"):
        """
        Fetch stock data for the previous calendar year using yfinance.
        """
        from datetime import date
        today = date.today()
        start = date(today.year - 1, 1, 1)
        end = date(today.year - 1, 12, 31)
        
        df = yf.download(symbol, start=start, end=end)
        df = df.reset_index()
        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }, inplace=True)
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        return df.to_dict(orient="records")


    @task
    def transform_data(data: list):
        for row in data:
            row["open"] = float(row["open"])
            row["high"] = float(row["high"])
            row["low"] = float(row["low"])
            row["close"] = float(row["close"])
            row["volume"] = int(row["volume"])
        return data

    @task
    def load_to_snowflake(data: list):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        df = pd.DataFrame(data)
        with hook.get_conn() as conn:
            conn.cursor().execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            conn.cursor().execute(f"USE DATABASE {SNOWFLAKE_DB}")
            conn.cursor().execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            # Optionally clear the table
            conn.cursor().execute(f"TRUNCATE TABLE {SNOWFLAKE_TABLE}")
            # Bulk insert
            from snowflake.connector.pandas_tools import write_pandas
            write_pandas(conn, df, table_name=SNOWFLAKE_TABLE, schema=SNOWFLAKE_SCHEMA, database=SNOWFLAKE_DB)

    # DAG dependencies
    stock_data = fetch_stock_data()
    transformed_data = transform_data(stock_data)
    load_to_snowflake(transformed_data)
