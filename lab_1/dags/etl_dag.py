from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import yfinance as yf
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import os

# Snowflake settings
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB", "USER_DB_BISON")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "BISON_QUERY_WH")
SNOWFLAKE_TABLE = "STOCK_DATA"

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
    def fetch_stock_data(symbols=["MSFT", "AAPL"], period="1y"):
        all_data = []

        for symbol in symbols:
            df = yf.download(symbol, period=period, auto_adjust=False)
            df.reset_index(inplace=True)

            # Flatten multi-index columns
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

            # Rename columns
            df.rename(
                columns={
                    "Date": "DATE",
                    "Open": "OPEN",
                    "High": "HIGH",
                    "Low": "LOW",
                    "Close": "CLOSE",
                    "Adj Close": "ADJ_CLOSE",
                    "Volume": "VOLUME"
                },
                inplace=True
            )

            # Ensure numeric columns
            for col in ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Convert DATE to string for XCom
            df["DATE"] = df["DATE"].dt.strftime("%Y-%m-%d")

            # Add symbol column (optional)
            df["SYMBOL"] = symbol

            # Convert to list of dicts (JSON-serializable)
            all_data.extend(df.to_dict(orient="records"))

        return all_data

    @task
    def transform_data(data: list):
        for row in data:
            for col in ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE"]:
                if col in row and row[col] is not None:
                    row[col] = float(row[col])
            if "VOLUME" in row and row["VOLUME"] is not None:
                row["VOLUME"] = int(row["VOLUME"])
        return data

    @task
    def load_to_snowflake(data: list):
        df = pd.DataFrame(data)

        # Convert DATE column to datetime.date
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.date

        # Drop columns not in Snowflake table
        df = df[["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]

        # Uppercase columns for Snowflake
        df.columns = [col.upper() for col in df.columns]

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        with hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
            cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            cur.execute(f"TRUNCATE TABLE {SNOWFLAKE_TABLE}")

            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name=SNOWFLAKE_TABLE,
                schema=SNOWFLAKE_SCHEMA,
                database=SNOWFLAKE_DB
            )

            print(f"✅ Uploaded {nrows} rows in {nchunks} chunks (Success={success})")

        return f"Loaded {len(df)} rows into Snowflake table {SNOWFLAKE_TABLE}."

    # DAG task dependencies
    raw_data = fetch_stock_data()
    transformed_data = transform_data(raw_data)
    load_to_snowflake(transformed_data)
