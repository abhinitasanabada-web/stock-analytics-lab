# dags/dag_yfinance_etl.py
# Airflow DAG #1 — yfinance → Snowflake (ETL)
# - Keeps ALL Snowflake secrets (account/user/password/warehouse/database/role) in the Airflow Connection: snowflake_catfish
# - Robust yfinance handling:
# - Transactional MERGE into RAW.STOCK_PRICES

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

default_args = {"depends_on_past": False, "retries": 1}

def _sf_connect(default_schema: str | None = None):
    """
    Build a snowflake.connector connection using the Airflow Connection.
    Put account/warehouse/database/schema/role in the Connection's Extra (UI).
    """
    c = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = c.extra_dejson or {}
    params = {
        "account": extra.get("account"),
        "user": c.login,
        "password": c.password,
        "warehouse": extra.get("warehouse"),
        "database": extra.get("database"),
        "schema": extra.get("schema", default_schema),
        "role": extra.get("role"),
    }
    # Drop None keys
    params = {k: v for k, v in params.items() if v is not None}
    return snowflake.connector.connect(**params)

with DAG(
    dag_id="yfinance_etl",
    schedule="@daily", # Changed schedule_interval to schedule
    start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["etl", "yfinance", "snowflake"],
) as dag:

    def ensure_objects():
        """
        Create RAW schema and RAW.STOCK_PRICES if not exists (transactional).
        Schema name comes from Airflow Variable 'target_schema_raw' (default RAW).
        """
        schema_raw = Variable.get("target_schema_raw", default_var="RAW")
        conn = _sf_connect(default_schema=schema_raw)
        cur = conn.cursor()
        try:
            conn.autocommit(False)
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_raw}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema_raw}.STOCK_PRICES (
                  SYMBOL STRING NOT NULL,
                  TS TIMESTAMP_NTZ NOT NULL,
                  OPEN FLOAT,
                  HIGH FLOAT,
                  LOW FLOAT,
                  CLOSE FLOAT,
                  ADJ_CLOSE FLOAT,
                  VOLUME NUMBER(38,0),
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
        Download recent OHLCV via yfinance, then MERGE into RAW.STOCK_PRICES (transactional).
        Fixes:
          - auto_adjust=False to retain 'Adj Close'
          - flatten MultiIndex columns
          - synthesize 'Adj Close' if missing
          - drop rows with missing CLOSE
        """
        schema_raw = Variable.get("target_schema_raw", default_var="RAW")
        symbols = json.loads(Variable.get("stock_symbols"))
        lookback_days = int(Variable.get("lookback_days", default_var="365"))

        end: date = datetime.now(timezone.utc).date()
        start: date = end - timedelta(days=lookback_days)

        frames: list[pd.DataFrame] = []

        for sym in symbols:
            df = yf.download(
                sym,
                start=str(start),
                end=str(end),
                auto_adjust=False,        # keep 'Adj Close'
                progress=False,
                group_by="column"         # sometimes returns MultiIndex
            )

            if df.empty:
                continue

            # Flatten MultiIndex columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [
                    " ".join([c for c in tup if c]).strip() for tup in df.columns
                ]

            # Ensure 'Adj Close' exists; if not, fall back to 'Close'
            if "Adj Close" not in df.columns and "Close" in df.columns:
                df["Adj Close"] = df["Close"]

            # Standardize column names and types
            df = df.reset_index().rename(
                columns={
                    "Date": "TS",
                    "Open": "OPEN",
                    "High": "HIGH",
                    "Low": "LOW",
                    "Close": "CLOSE",
                    "Adj Close": "ADJ_CLOSE",
                    "Volume": "VOLUME",
                }
            )

            # Drop rows without CLOSE to avoid type issues on insert
            if "CLOSE" not in df.columns:
                # extremely rare; skip this symbol if no CLOSE at all
                continue
            df = df.dropna(subset=["CLOSE"])

            # Ensure TS is datetime (yfinance returns Timestamp already, but be explicit)
            df["TS"] = pd.to_datetime(df["TS"])

            df["SYMBOL"] = sym

            # Keep only required columns (some may be NaN which is okay for FLOAT)
            required_cols = ["SYMBOL", "TS", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]
            # Add any missing optional numeric columns as NaN to keep schema consistent
            for col in required_cols:
                if col not in df.columns:
                    df[col] = pd.NA

            frames.append(df[required_cols])

        if not frames:
            return "No data downloaded."

        data = pd.concat(frames, ignore_index=True)

        # Upload -> MERGE (transactional)
        conn = _sf_connect(default_schema=schema_raw)
        cur = conn.cursor()
        try:
            conn.autocommit(False)

            # Temp table to batch insert
            cur.execute("""
                CREATE TEMP TABLE TMP_LOAD (
                  SYMBOL STRING,
                  TS TIMESTAMP_NTZ,
                  OPEN FLOAT,
                  HIGH FLOAT,
                  LOW FLOAT,
                  CLOSE FLOAT,
                  ADJ_CLOSE FLOAT,
                  VOLUME NUMBER(38,0)
                )
            """)

            # Convert dataframe to list-of-tuples for executemany
            rows = list(data.itertuples(index=False, name=None))
            cur.executemany(
                "INSERT INTO TMP_LOAD VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                rows,
            )

            # MERGE into target
            cur.execute(f"""
                MERGE INTO {schema_raw}.STOCK_PRICES t
                USING TMP_LOAD s
                ON  t.SYMBOL = s.SYMBOL
                AND t.TS     = s.TS
                WHEN MATCHED THEN UPDATE SET
                  OPEN      = s.OPEN,
                  HIGH      = s.HIGH,
                  LOW       = s.LOW,
                  CLOSE     = s.CLOSE,
                  ADJ_CLOSE = s.ADJ_CLOSE,
                  VOLUME    = s.VOLUME,
                  LOAD_TS   = CURRENT_TIMESTAMP()
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
