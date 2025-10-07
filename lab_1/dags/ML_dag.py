# airflow_ml_forecast_dag_fixed_v3.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
from prophet import Prophet
from snowflake.connector.pandas_tools import write_pandas
import os

# Snowflake settings
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DB", "USER_DB_BISON")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "BISON_QUERY_WH")

RAW_TABLE = "STOCK_DATA"
FORECAST_TABLE = "STOCK_FORECAST"
FINAL_TABLE = "STOCK_FINAL"

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    "ml_forecast_to_snowflake_fixed",
    default_args=default_args,
    start_date=datetime(2025, 10, 3),
    schedule="@daily",
    catchup=False,
) as dag:

    @task
    def create_tables():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        with hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
                    DATE DATE,
                    OPEN FLOAT,
                    HIGH FLOAT,
                    LOW FLOAT,
                    CLOSE FLOAT,
                    VOLUME NUMBER
                )
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {FORECAST_TABLE} (
                    DATE DATE PRIMARY KEY,
                    FORECAST_CLOSE FLOAT,
                    MODEL_USED STRING
                )
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {FINAL_TABLE} (
                    DATE DATE,
                    CLOSE FLOAT,
                    FORECAST_CLOSE FLOAT
                )
            """)
            cur.close()

    @task
    def fetch_raw_data():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        with hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", conn)

        if df.empty:
            raise ValueError("❌ No data found in STOCK_DATA table")

        # Convert DATE column to datetime then string for Airflow XCom
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.strftime("%Y-%m-%d")
        return df.to_dict(orient="records")

    @task
    def train_forecast(data: list):
        df = pd.DataFrame(data)
        df.columns = [str(c).lower() for c in df.columns]
        if "date" not in df.columns or "close" not in df.columns:
            raise ValueError(f"❌ Missing required columns. Found: {df.columns.tolist()}")
        df_prophet = df[["date", "close"]].rename(columns={"date": "ds", "close": "y"})
        model = Prophet()
        model.fit(df_prophet)
        future = model.make_future_dataframe(periods=30)
        forecast = model.predict(future)
        forecast_df = forecast[["ds", "yhat"]].rename(columns={"ds": "date", "yhat": "forecast_close"})
        forecast_df["model_used"] = "Prophet"
        # Convert date to string for XCom
        forecast_df["date"] = forecast_df["date"].dt.strftime("%Y-%m-%d")
        return forecast_df.to_dict(orient="records")

    @task
    def load_forecast(data):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        df = pd.DataFrame(data)
        
        # Rename columns to match Snowflake table
        df.rename(columns={
            "date": "DATE",
            "forecast_close": "FORECAST_CLOSE",
            "model_used": "MODEL_USED"
        }, inplace=True)
        
        with hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            
            # Ensure table exists
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {FORECAST_TABLE} (
                    DATE DATE PRIMARY KEY,
                    FORECAST_CLOSE FLOAT,
                    MODEL_USED STRING
                )
            """)
            
            # Optional: clear table
            cur.execute(f"TRUNCATE TABLE {FORECAST_TABLE}")
            
            # Upload
            write_pandas(conn, df, table_name=FORECAST_TABLE, schema=SNOWFLAKE_SCHEMA, database=SNOWFLAKE_DATABASE)


    @task
    def union_final():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        with hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            cur.execute(f"""
                CREATE OR REPLACE TABLE {FINAL_TABLE} AS
                SELECT r.date, r.close, f.forecast_close
                FROM {RAW_TABLE} r
                LEFT JOIN {FORECAST_TABLE} f
                ON r.date = f.date
            """)

    # DAG dependencies
    tables = create_tables()
    raw_data = fetch_raw_data()
    forecast = train_forecast(raw_data)
    loaded = load_forecast(forecast)
    final = union_final()

    tables >> raw_data >> forecast >> loaded >> final
