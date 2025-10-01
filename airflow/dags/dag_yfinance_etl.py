# dags/dag_yfinance_etl.py
# Simple, reliable ETL: yfinance -> Snowflake via write_pandas (staging) -> DELETE→INSERT upsert
# Airflow Variables expected:
#   - stock_symbols: JSON array, e.g. ["AAPL","MSFT","TSLA"]
#   - lookback_days: e.g. 365
#   - target_schema_raw: default "RAW"
#
# Airflow Connection "snowflake_catfish" (Conn Type: Snowflake) Extra example:
# {
#   "account": "LVB17920",
#   "warehouse": "CAT_QUERY_WH",
#   "database": "USER_DB_CATFISH",
#   "role": "TRAINING_ROLE",
#   "insecure_mode": true
# }

import json
from datetime import datetime, date, timedelta, timezone as dt_tz
from typing import Optional, List

import pandas as pd
import yfinance as yf
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

SNOWFLAKE_CONN_ID = "snowflake_catfish"
default_args = {"depends_on_past": False, "retries": 1}


def _sf_connect(default_schema: Optional[str] = None):
    """Build a Snowflake connection from Airflow Connection extras."""
    c = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = c.extra_dejson or {}
    params = {
        "account":        extra.get("account"),
        "user":           c.login,
        "password":       c.password,
        "warehouse":      extra.get("warehouse"),
        "database":       extra.get("database"),
        "schema":         extra.get("schema", default_schema),
        "role":           extra.get("role"),
        "insecure_mode":  extra.get("insecure_mode", False),
    }
    params = {k: v for k, v in params.items() if v is not None}
    return snowflake.connector.connect(**params)


with DAG(
    dag_id="yfinance_etl",
    schedule="@daily",  # trigger daily
    start_date=datetime(2025, 9, 1, tzinfo=dt_tz.utc),
    catchup=False,
    default_args=default_args,
    tags=["etl", "yfinance", "snowflake"],
) as dag:

    def ensure_objects():
        """Create RAW schema, RAW.STOCK_PRICES (PK), and RAW.STOCK_PRICES_STAGE (no PK)."""
        schema_raw = Variable.get("target_schema_raw", default_var="RAW")
        conn = _sf_connect(default_schema=schema_raw)
        cur = conn.cursor()
        try:
            conn.autocommit(False)

            cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
            db, sch, role, wh = cur.fetchone()
            print(f"[BOOT] DB={db} SCHEMA={sch} ROLE={role} WH={wh}", flush=True)

            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema_raw}")

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {db}.{schema_raw}.STOCK_PRICES (
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

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {db}.{schema_raw}.STOCK_PRICES_STAGE (
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

            conn.commit()
            print(f"[DDL] Ensured {db}.{schema_raw}.STOCK_PRICES and {db}.{schema_raw}.STOCK_PRICES_STAGE.", flush=True)
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def fetch_and_load_prices():
        """
        Fetch prices with yfinance Ticker().history() (stable columns),
        stage with write_pandas, then DELETE→INSERT upsert into RAW.STOCK_PRICES.
        """
        schema_raw = Variable.get("target_schema_raw", default_var="RAW")

        # Variables
        try:
            symbols = json.loads(Variable.get("stock_symbols"))
            assert isinstance(symbols, list) and all(isinstance(x, str) for x in symbols)
        except Exception as e:
            raise ValueError('Airflow Variable "stock_symbols" must be JSON like ["AAPL","MSFT","TSLA"].') from e

        lookback_days = int(Variable.get("lookback_days", default_var="365"))
        end: date = datetime.now(dt_tz.utc).date()
        start: date = end - timedelta(days=lookback_days)
        print(f"[CFG] symbols={symbols} start={start} end={end} (history end is inclusive)", flush=True)

        frames: List[pd.DataFrame] = []

        for sym in symbols:
            t = yf.Ticker(sym)

            # Primary fetch: explicit start/end
            h = t.history(start=str(start), end=str(end), interval="1d", auto_adjust=False)
            if h.empty:
                # Fallback fetch: recent period
                h = t.history(period="60d", interval="1d", auto_adjust=False)
                print(f"[YF] {sym}: empty for start/end; retry period=60d → empty={h.empty}", flush=True)

            if h.empty:
                continue

            # Expected columns: Open, High, Low, Close, Volume (+/- Adj Close)
            h = h.reset_index()
            df = h.rename(columns={
                "Date": "TS",
                "Open": "OPEN",
                "High": "HIGH",
                "Low": "LOW",
                "Close": "CLOSE",
                "Adj Close": "ADJ_CLOSE",
                "Volume": "VOLUME",
            })

            if "ADJ_CLOSE" not in df.columns:
                df["ADJ_CLOSE"] = df["CLOSE"]

            df["TS"] = pd.to_datetime(df["TS"], utc=True, errors="coerce")
            for c in ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

            df["SYMBOL"] = sym
            df = df[["SYMBOL", "TS", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]
            df = df.dropna(subset=["TS", "CLOSE"])

            if not df.empty:
                print(f"[YF] {sym}: rows={len(df)} TS[{df['TS'].min()}..{df['TS'].max()}]", flush=True)
                frames.append(df)
            else:
                print(f"[YF] {sym}: normalized frame empty after dropna; skipping.", flush=True)

        if not frames:
            print("[LOAD] No data to load (all tickers empty).", flush=True)
            return "No data to load."

        data = pd.concat(frames, ignore_index=True)

        # Ensure NUMBER column is int/NULL for Snowflake
        data["VOLUME"] = data["VOLUME"].apply(lambda v: None if pd.isna(v) else int(v))

        # Stage and upsert
        conn = _sf_connect(default_schema=schema_raw)
        cur = conn.cursor()
        try:
            conn.autocommit(False)

            cur.execute("SELECT CURRENT_DATABASE()")
            db = cur.fetchone()[0]
            stage  = f"{db}.{schema_raw}.STOCK_PRICES_STAGE"
            target = f"{db}.{schema_raw}.STOCK_PRICES"

            cur.execute(f"TRUNCATE TABLE {stage}")
            ok, chunks, nrows, _ = write_pandas(
                conn, data,
                table_name="STOCK_PRICES_STAGE",
                database=db, schema=schema_raw,
                auto_create_table=False, quote_identifiers=False
            )
            print(f"[STAGE] write_pandas ok={ok} rows={nrows} chunks={chunks}", flush=True)

            # DELETE→INSERT upsert (PK on SYMBOL,TS avoids dupes)
            cur.execute(f"""
                DELETE FROM {target} t
                USING {stage} s
                WHERE t.SYMBOL = s.SYMBOL
                  AND t.TS     = s.TS
            """)
            try:
                cur.execute("SELECT SQLROWCOUNT()")
                print(f"[UPSERT] Deleted: {cur.fetchone()[0]}", flush=True)
            except Exception:
                pass

            cur.execute(f"""
                INSERT INTO {target}
                    (SYMBOL, TS, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME)
                SELECT SYMBOL, TS, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME
                FROM {stage}
            """)
            try:
                cur.execute("SELECT SQLROWCOUNT()")
                print(f"[UPSERT] Inserted: {cur.fetchone()[0]}", flush=True)
            except Exception:
                pass

            conn.commit()

            cur.execute(f"SELECT COUNT(*) FROM {target}")
            total = cur.fetchone()[0]
            print(f"[DONE] Total rows now in {target}: {total}", flush=True)
            return f"Loaded {len(data)} rows into {target}"
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    ensure = PythonOperator(task_id="ensure_objects", python_callable=ensure_objects)
    load   = PythonOperator(task_id="fetch_and_load_prices", python_callable=fetch_and_load_prices)

    ensure >> load
