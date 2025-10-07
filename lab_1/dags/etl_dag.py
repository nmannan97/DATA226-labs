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
        import yfinance as yf
        import pandas as pd

        df = yf.download(symbol, period=period)
        df = df.reset_index()

        # ✅ Flatten multi-index column names
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
        print(f"✅ Columns after flattening: {df.columns.tolist()}")

        # ✅ Rename columns to match Snowflake table
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

        print(f"✅ Columns after rename: {df.columns.tolist()}")

        # ✅ Ensure DATE column is clean
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.strftime("%Y-%m-%d")

        # ✅ Convert numeric columns safely
        numeric_cols = ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                print(f"⚠️ Missing column: {col}")

        print(f"✅ Sample rows:\n{df.head()}")

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
        import pandas as pd
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from snowflake.connector.pandas_tools import write_pandas

        # Create DataFrame from list of dicts
        df = pd.DataFrame(data)

        # ✅ Rename the date column to match Snowflake table
        if "trade_date" in df.columns:
            df.rename(columns={"trade_date": "DATE"}, inplace=True)

        # ✅ Make sure all columns are uppercase (Snowflake default)
        df.columns = [col.upper() for col in df.columns]

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

        with hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
            cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            
            # ✅ Use valid truncate syntax
            cur.execute(f"TRUNCATE TABLE {SNOWFLAKE_TABLE}")

            # ✅ Upload using Snowflake's optimized write_pandas
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