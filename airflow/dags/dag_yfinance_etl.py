# Airflow DAG #1 — yfinance → Snowflake (ETL)
# Secrets (account/user/password/warehouse/database/role/schema) come from the Airflow Connection: snowflake_catfish.
# Uses snowflake.connector directly (no Airflow SnowflakeHook).

import json
from datetime import datetime, date, timedelta, timezone

import pandas as pd
import yfinance as yf
import snowflake.connector

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

SNOWFLAKE_CONN_ID = "snowflake_catfish"
SCHEMA_RAW = Variable.get("target_schema_raw", default_var="RAW")

default_args = {"depends_on_past": False, "retries": 1}

def _sf_connect():
    """
    Build a snowflake.connector connection using the Airflow Connection.
    Ensure your Airflow Snowflake connection has account, warehouse, database,
    (optional) schema/role in the 'Extra' JSON.
    """
    c = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = c.extra_dejson or {}
    params = {
        "account": extra.get("account"),
        "user": c.login,
        "password": c.password,
        "warehouse": extra.get("warehouse"),
        "database": extra.get("database"),
        # set a default schema if the connection doesn't already have one
        "schema": extra.get("schema", SCHEMA_RAW),
        "role": extra.get("role"),
        # Optional niceties:
        # "client_session_keep_alive": True,
    }
    # Remove any Nones
    params = {k: v for k, v in params.items() if v is not None}
    return snowflake.connector.connect(**params)

with DAG(
    dag_id="yfinance_etl",
    schedule="@daily", # Changed schedule_interval to schedule
    start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),  # timezone-aware
    catchup=False,
    default_args=default_args,
    tags=["etl", "yfinance", "snowflake"],
) as dag:

    def ensure_objects():
        """Create RAW schema and STOCK_PRICES table if not exists (transactional)."""
        conn = _sf_connect()
        cur = conn.cursor()
        try:
            conn.autocommit(False)
            # If your connection already sets a default schema, CREATE SCHEMA IF NOT EXISTS is harmless.
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA_RAW}.STOCK_PRICES (
                  SYMBOL STRING NOT NULL,
                  TS TIMESTAMP_NTZ NOT NULL,
                  OPEN FLOAT, HIGH FLOAT, LOW FLOAT, CLOSE FLOAT,
                  ADJ_CLOSE FLOAT, VOLUME NUMBER(38,0),
                  LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                  CONSTRAINT PK_STOCK_PRICES PRIMARY KEY (SYMBOL, TS)
                )
            """)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    create_objs = PythonOperator(
        task_id="create_schema_tables",
        python_callable=ensure_objects,
    )

    def fetch_and_upsert():
        """
        Download recent OHLCV via yfinance for symbols in Airflow Variable,
        and MERGE into RAW.STOCK_PRICES (transactional) using snowflake.connector.
        """
        symbols = json.loads(Variable.get("stock_symbols"))
        lookback_days = int(Variable.get("lookback_days", default_var="365"))

        end: date = datetime.now(timezone.utc).date()
        start: date = end - timedelta(days=lookback_days)

        frames = []
        for sym in symbols:
            df = yf.download(sym, start=str(start), end=str(end), auto_adjust=True, progress=False)
            if df.empty:
                continue
            df = df.reset_index().rename(columns={
                "Date": "TS",
                "Open": "OPEN",
                "High": "HIGH",
                "Low": "LOW",
                "Close": "CLOSE",
                "Adj Close": "ADJ_CLOSE",
                "Volume": "VOLUME",
            })
            df["SYMBOL"] = sym
            frames.append(df[["SYMBOL", "TS", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]])

        if not frames:
            return "No data downloaded."

        data = pd.concat(frames, ignore_index=True)

        conn = _sf_connect()
        cur = conn.cursor()
        try:
            conn.autocommit(False)

            # Stage into temp table, then MERGE into RAW.STOCK_PRICES
            rows = list(data.itertuples(index=False, name=None))
            cur.execute("""
                CREATE TEMP TABLE TMP_LOAD (
                  SYMBOL STRING, TS TIMESTAMP_NTZ,
                  OPEN FLOAT, HIGH FLOAT, LOW FLOAT, CLOSE FLOAT,
                  ADJ_CLOSE FLOAT, VOLUME NUMBER(38,0)
                )
            """)
            cur.executemany(
                "INSERT INTO TMP_LOAD VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                rows,
            )

            cur.execute(f"""
                MERGE INTO {SCHEMA_RAW}.STOCK_PRICES t
                USING TMP_LOAD s
                ON t.SYMBOL = s.SYMBOL AND t.TS = s.TS
                WHEN MATCHED THEN UPDATE SET
                  OPEN = s.OPEN,
                  HIGH = s.HIGH,
                  LOW = s.LOW,
                  CLOSE = s.CLOSE,
                  ADJ_CLOSE = s.ADJ_CLOSE,
                  VOLUME = s.VOLUME,
                  LOAD_TS = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                  (SYMBOL, TS, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME)
                  VALUES (s.SYMBOL, s.TS, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.ADJ_CLOSE, s.VOLUME)
            """)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    upsert_prices = PythonOperator(
        task_id="fetch_and_upsert_prices",
        python_callable=fetch_and_upsert,
    )

    create_objs >> upsert_prices
