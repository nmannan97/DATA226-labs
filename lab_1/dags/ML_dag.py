# airflow_ml_forecast_dag.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
from prophet import Prophet

SNOWFLAKE_DATABASE = "USER_DB_BISON"
SNOWFLAKE_SCHEMA = "PUBLIC"
RAW_TABLE = "STOCK_DATA_RAW"
FORECAST_TABLE = "STOCK_DATA_FORECAST"
FINAL_TABLE = "STOCK_DATA_FINAL"
SNOWFLAKE_WAREHOUSE = "BISON_QUERY_WH"

default_args = {"owner": "airflow", "retries": 1}

with DAG("ml_forecast_to_snowflake", default_args=default_args, start_date=datetime(2025,10,3), schedule="@daily", catchup=False) as dag:

    @task
    def fetch_raw_data():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        with hook.get_conn() as conn:
            df = pd.read_sql(f"SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{RAW_TABLE}", conn)
        return df.to_dict(orient="records")

    @task
    def train_forecast(data):
        df = pd.DataFrame(data)
        df_prophet = df[["date","close"]].rename(columns={"date":"ds","close":"y"})
        model = Prophet()
        model.fit(df_prophet)
        future = model.make_future_dataframe(periods=30)
        forecast = model.predict(future)
        forecast_df = forecast[["ds","yhat"]].rename(columns={"ds":"date","yhat":"forecast_close"})
        forecast_df["model_used"] = "Prophet"
        return forecast_df.to_dict(orient="records")

    @task
    def load_forecast(data):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
                cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
                cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {FORECAST_TABLE} (
                        date DATE PRIMARY KEY,
                        forecast_close FLOAT,
                        model_used STRING
                    )
                """)
                cur.execute(f"TRUNCATE TABLE {FORECAST_TABLE}")
                for row in data:
                    cur.execute(
                        f"INSERT INTO {FORECAST_TABLE} (date, forecast_close, model_used) VALUES (%s,%s,%s)",
                        (row["date"], row["forecast_close"], row["model_used"])
                    )

    @task
    def union_final():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        with hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {FINAL_TABLE} AS
                SELECT r.date, r.close, f.forecast_close
                FROM {RAW_TABLE} r
                LEFT JOIN {FORECAST_TABLE} f ON r.date=f.date
            """)

    raw_data = fetch_raw_data()
    forecast = train_forecast(raw_data)
    load_forecast(forecast)
    union_final()
