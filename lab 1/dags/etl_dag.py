# stock_data_dag.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from dotenv import load_dotenv
import os
import requests
import pandas as pd

# Load .env variables
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = "STOCK_DATA"

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "stock_data_to_snowflake",
    default_args=default_args,
    start_date=datetime(2025, 10, 3),
    schedule="@daily",
    catchup=False,
) as dag:

    @task
    def fetch_stock_data(symbol="MSFT"):
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()["Time Series (Daily)"]
        df = pd.DataFrame(data).T.reset_index()
        df.columns = ["date", "open", "high", "low", "close", "volume"]
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
        hook = SnowflakeHook(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA,
        )
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                full_table = f"{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
                try:
                    cur.execute("USE WAREHOUSE " + SNOWFLAKE_WAREHOUSE)
                    cur.execute("BEGIN;")
                    cur.execute(f"TRUNCATE TABLE {full_table};")
                    for row in data:
                        cur.execute(
                            f"""
                            INSERT INTO {full_table} (DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
                            VALUES (%s, %s, %s, %s, %s, %s);
                            """,
                            (row["date"], row["open"], row["high"], row["low"], row["close"], row["volume"])
                        )
                    cur.execute("COMMIT;")
                except Exception as e:
                    cur.execute("ROLLBACK;")
                    raise e

    # Task dependencies
    stock_data = fetch_stock_data()
    transformed_data = transform_data(stock_data)
    load_to_snowflake(transformed_data)
