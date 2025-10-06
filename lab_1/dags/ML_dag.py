# airflow_ml_forecast_dag_fixed_v2.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
from prophet import Prophet
from snowflake.connector.pandas_tools import write_pandas

# Constants
RAW_TABLE = "STOCK_DATA_RAW"
FORECAST_TABLE = "STOCK_DATA_FORECAST"
FINAL_TABLE = "STOCK_DATA_FINAL"
SNOWFLAKE_DATABASE = "USER_DB_BISON"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "BISON_QUERY_WH"

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

            # Raw data table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{RAW_TABLE} (
                    DATE DATE,
                    OPEN FLOAT,
                    HIGH FLOAT,
                    LOW FLOAT,
                    CLOSE FLOAT,
                    VOLUME NUMBER
                )
            """)

            # Forecast table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{FORECAST_TABLE} (
                    DATE DATE PRIMARY KEY,
                    FORECAST_CLOSE FLOAT,
                    MODEL_USED STRING
                )
            """)

            # Final table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{FINAL_TABLE} (
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
            conn.cursor().execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            conn.cursor().execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            conn.cursor().execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            df = pd.read_sql(
                f"SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{RAW_TABLE}", conn
            )
        return df.to_dict(orient="records")

    @task
    def train_forecast(data):
        df = pd.DataFrame(data)
        df_prophet = df[["date", "close"]].rename(columns={"date": "ds", "close": "y"})
        model = Prophet()
        model.fit(df_prophet)
        future = model.make_future_dataframe(periods=30)
        forecast = model.predict(future)
        forecast_df = forecast[["ds", "yhat"]].rename(columns={"ds": "date", "yhat": "forecast_close"})
        forecast_df["model_used"] = "Prophet"
        return forecast_df.to_dict(orient="records")

    @task
    def load_forecast(data):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        df = pd.DataFrame(data)
        with hook.get_conn() as conn:
            conn.cursor().execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            conn.cursor().execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            conn.cursor().execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            # Make sure table exists
            conn.cursor().execute(f"""
                CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{FORECAST_TABLE} (
                    date DATE PRIMARY KEY,
                    forecast_close FLOAT,
                    model_used STRING
                )
            """)
            # Optional: clear table
            conn.cursor().execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{FORECAST_TABLE}")
            # Upload
            write_pandas(conn, df, table_name=FORECAST_TABLE, schema=SNOWFLAKE_SCHEMA, database=SNOWFLAKE_DATABASE)

    @task
    def union_final():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        with hook.get_conn() as conn:
            conn.cursor().execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            conn.cursor().execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            conn.cursor().execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            conn.cursor().execute(f"""
                CREATE OR REPLACE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{FINAL_TABLE} AS
                SELECT r.date, r.close, f.forecast_close
                FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{RAW_TABLE} r
                LEFT JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{FORECAST_TABLE} f
                ON r.date = f.date
            """)

    tables = create_tables()
    raw_data = fetch_raw_data()
    forecast = train_forecast(raw_data)
    loaded = load_forecast(forecast)
    combine = union_final()

    tables >> raw_data >> forecast >> loaded >> combine