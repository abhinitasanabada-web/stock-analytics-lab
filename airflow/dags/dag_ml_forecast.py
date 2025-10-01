# dags/dag_ml_forecast_tf.py
# Airflow DAG #2 — ML Forecasting → Snowflake (Union with ETL)
# - No heavy ML deps at parse-time (TensorFlow/sklearn kept out)
# - Creates/ensures schemas and tables
# - Forecasts per-ticker with a simple MA5 extrapolation
# - Writes to ML.FORECAST_OUTPUT and builds ANALYTICS.STOCK_PRICES_FINAL (union)

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone, date

import numpy as np
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

SNOWFLAKE_CONN_ID = "snowflake_catfish"

# ---------- Airflow Variables (with sane defaults) ----------
# Comma-separated tickers, e.g. "AAPL,MSFT"
VAR_TICKERS = Variable.get("tickers", default_var="AAPL,MSFT")
# Lookback window for training (days)
VAR_LOOKBACK_DAYS = int(Variable.get("forecast_lookback_days", default_var="365"))
# Forecast horizon (business days)
VAR_FORECAST_DAYS = int(Variable.get("forecast_days", default_var="14"))

# Schema/table names (override via Variables if your naming differs)
SCHEMA_RAW = Variable.get("schema_raw", default_var="RAW")
SCHEMA_ML = Variable.get("schema_ml", default_var="ML")
SCHEMA_ANALYTICS = Variable.get("schema_analytics", default_var="ANALYTICS")

TABLE_PRICES = Variable.get("table_prices", default_var="STOCK_PRICES")
TABLE_FORECAST = Variable.get("table_forecast", default_var="FORECAST_OUTPUT")
TABLE_FINAL = Variable.get("table_final", default_var="STOCK_PRICES_FINAL")

# ---------- Helpers ----------
def _sf_connect(default_schema: str | None = None):
    """
    Create a snowflake.connector connection using Airflow Connection extras.
    Place account/warehouse/database/role in Connection's 'Extra' JSON.
    """
    import snowflake.connector  # safe at parse time

    c = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = c.extra_dejson or {}
    params = {
        "account":   extra.get("account"),
        "user":      c.login,
        "password":  c.password,
        "warehouse": extra.get("warehouse"),
        "database":  extra.get("database"),
        "role":      extra.get("role"),
    }
    if default_schema:
        params["schema"] = default_schema
    # Optional: allow insecure_mode in lab setups
    if "insecure_mode" in extra:
        params["insecure_mode"] = bool(extra.get("insecure_mode"))
    return snowflake.connector.connect(**params)

def _ensure_objects(**_):
    """
    Create required schemas and tables if they don't exist.
    RAW.STOCK_PRICES is assumed to be created by DAG #1.
    """
    conn = _sf_connect()
    try:
        with conn.cursor() as cur:
            # Schemas
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_ML}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_ANALYTICS}")

            # Forecast output table (per-ticker daily forecast)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA_ML}.{TABLE_FORECAST} (
                    TICKER        STRING,
                    DS            DATE,
                    FORECAST_CLOSE FLOAT,
                    MODEL_NAME    STRING,
                    TRAIN_WINDOW  INTEGER,
                    HORIZON_DAYS  INTEGER,
                    RUN_AT        TIMESTAMP_NTZ,
                    PRIMARY KEY (TICKER, DS)
                )
            """)

            # Final union table (will be (re)built later)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA_ANALYTICS}.{TABLE_FINAL} (
                    TICKER        STRING,
                    DS            DATE,
                    CLOSE         FLOAT,
                    SOURCE        STRING,          -- 'ETL' or 'FORECAST'
                    ADJ_CLOSE     FLOAT,
                    OPEN          FLOAT,
                    HIGH          FLOAT,
                    LOW           FLOAT,
                    VOLUME        FLOAT
                )
            """)
        conn.commit()
    finally:
        conn.close()

def _load_training_data(ti, **_):
    """
    Pull recent history from RAW.STOCK_PRICES for requested tickers.
    Push a JSON payload to XCom.
    """
    tickers = [t.strip().upper() for t in VAR_TICKERS.split(",") if t.strip()]
    since = (date.today() - timedelta(days=VAR_LOOKBACK_DAYS)).isoformat()

    sql = f"""
        SELECT
            TICKER,
            CAST(DATE AS DATE) AS DS,
            CLOSE,
            ADJ_CLOSE,
            OPEN, HIGH, LOW, VOLUME
        FROM {SCHEMA_RAW}.{TABLE_PRICES}
        WHERE TICKER IN ({','.join(['%s'] * len(tickers))})
          AND DATE >= %s
        ORDER BY TICKER, DS
    """

    conn = _sf_connect(default_schema=SCHEMA_RAW)
    try:
        df = pd.read_sql(sql, conn, params=[*tickers, since])
    finally:
        conn.close()

    # Ensure types
    if not df.empty:
        df["DS"] = pd.to_datetime(df["DS"]).dt.date

    ti.xcom_push(key="training_df_json", value=df.to_json(orient="records", date_format="iso"))

def _train_and_forecast(ti, **_):
    """
    Simple per-ticker forecast using a moving-average extrapolation.
    Keeps deps minimal; heavy frameworks can be swapped in later.
    """
    # Load training set from XCom
    records_json = ti.xcom_pull(key="training_df_json", task_ids="load_training_data")
    df = pd.DataFrame.from_records(pd.read_json(records_json, orient="records")) if records_json else pd.DataFrame()

    if df.empty:
        # Nothing to forecast
        ti.xcom_push(key="forecast_df_json", value=pd.DataFrame().to_json(orient="records"))
        return

    df["DS"] = pd.to_d_
