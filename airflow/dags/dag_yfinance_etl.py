# dags/dag_yfinance_etl.py
# Airflow DAG #1 — yfinance → Snowflake (ETL) with strict date handling (no "Invalid date")
#
# Airflow Variables (Admin → Variables):
#   - stock_symbols       JSON, e.g. ["AAPL","MSFT","TSLA"] (default: AAPL,MSFT,TSLA)
#   - lookback_days       int, e.g. 365 (default: 365)
#   - target_schema_raw   str, e.g. "RAW" (default: RAW)
#
# Airflow Connection: snowflake_catfish (Conn Type: Snowflake)
#   Extra JSON example:
#   {
#     "account": "xxxx-xxxx",
#     "warehouse": "COMPUTE_WH",
#     "database": "YOUR_DB",
#     "schema": "RAW",
#     "role": "SYSADMIN"
#   }

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List


import pandas as pd
import yfinance as yf
import snowflake.connector

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import task

SNOWFLAKE_CONN_ID = "snowflake_catfish"
default_args = {"depends_on_past": False, "retries": 1}


def _sf_connect(schema_override: str | None = None):
    """Build a snowflake.connector connection from the Airflow Connection."""
    c = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = c.extra_dejson or {}
    params = {
        "account": extra.get("account"),
        "user": c.login,
        "password": c.password,
        "warehouse": extra.get("warehouse"),
        "database": extra.get("database"),
        "schema": schema_override or extra.get("schema"),
        "role": extra.get("role"),
        "client_session_keep_alive": True,
    }
    params = {k: v for k, v in params.items() if v is not None}
    return snowflake.connector.connect(**params)


def _fetch_prices(symbols: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch OHLCV with yfinance and return long-form DataFrame with:
      TRADE_DATE (date), SYMBOL, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME
    STRICT date handling: drops unparsable timestamps → no "Invalid date".
    """
    if not symbols:
        return pd.DataFrame(
            columns=["TRADE_DATE", "SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]
        )

    raw = yf.download(
        tickers=" ".join(symbols),
        start=start_date,
        end=end_date,
        auto_adjust=False,   # keep Close and Adj Close distinct
        group_by="ticker",
        progress=False,
        threads=True,
    )

    # Normalize possible MultiIndex columns
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = ["_".join([str(c) for c in tup if c]) for tup in raw.columns]

    # --- FIXED DATE HANDLING ---
    idx = pd.to_datetime(raw.index, errors="coerce").tz_localize(None)  # DatetimeIndex
    df_base = raw.reset_index(drop=True).copy()

    ts = pd.Series(idx, name="TRADE_TS", dtype="datetime64[ns]")  # Series to use .dt
    good = ts.notna()
    df_base = df_base.loc[good].copy()
    ts = ts.loc[good]
    # Keep as Python date objects (this is crucial for clean executemany binding)
    trade_dates = ts.dt.date
    # ---------------------------

    # Build long-form rows per symbol
    rows = []
    for sym in symbols:
        sym_u = str(sym).upper().strip()
        col_map = {
            "OPEN":      [f"{sym_u}_Open", "Open"],
            "HIGH":      [f"{sym_u}_High", "High"],
            "LOW":       [f"{sym_u}_Low", "Low"],
            "CLOSE":     [f"{sym_u}_Close", "Close"],
            "ADJ_CLOSE": [f"{sym_u}_Adj Close", "Adj Close", f"{sym_u}_Adj_Close", "Adj_Close"],
            "VOLUME":    [f"{sym_u}_Volume", "Volume"],
        }

        def pick(colnames):
            for c in colnames:
                if c in df_base.columns:
                    return df_base[c]
            return pd.Series([None] * len(df_base), index=df_base.index)

        df_sym = pd.DataFrame({
            "TRADE_DATE": trade_dates,  # Python date
            "SYMBOL":     sym_u,
            "OPEN":       pick(col_map["OPEN"]),
            "HIGH":       pick(col_map["HIGH"]),
            "LOW":        pick(col_map["LOW"]),
            "CLOSE":      pick(col_map["CLOSE"]),
            "ADJ_CLOSE":  pick(col_map["ADJ_CLOSE"]),
            "VOLUME":     pick(col_map["VOLUME"]),
        })

        # Synthesize ADJ_CLOSE if missing
        if df_sym["ADJ_CLOSE"].isna().all():
            df_sym["ADJ_CLOSE"] = df_sym["CLOSE"]

        # Drop rows without CLOSE (prevents type issues downstream)
        df_sym = df_sym[df_sym["CLOSE"].notna()].copy()
        rows.append(df_sym)

    if not rows:
        return pd.DataFrame(
            columns=["TRADE_DATE", "SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]
        )

    df = pd.concat(rows, ignore_index=True)

    # Enforce types
    df["SYMBOL"] = df["SYMBOL"].astype(str).str.upper().str.strip()
    for col in ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["VOLUME"] = pd.to_numeric(df["VOLUME"], errors="coerce").astype("Int64")

    # Final clean
    df = df.dropna(subset=["TRADE_DATE", "SYMBOL", "CLOSE"]).copy()
    df = df.drop_duplicates(subset=["TRADE_DATE", "SYMBOL"]).sort_values(["SYMBOL", "TRADE_DATE"])

    logging.info("Fetch summary: %d rows for symbols=%s (%s → %s)",
                 len(df), symbols, start_date, end_date)
    return df[["TRADE_DATE", "SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]

@task
def ensure_objects(**context):
    """Ensure RAW schema/table exist with strict DATE typing (not VARCHAR)."""
    schema = Variable.get("target_schema_raw", default_var="RAW")
    with _sf_connect(schema) as conn, conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cur.execute(f"""
        CREATE OR REPLACE TABLE "{schema}"."STOCK_PRICES" (
          TRADE_DATE DATE NOT NULL,
          SYMBOL     STRING NOT NULL,
          OPEN       DOUBLE,
          HIGH       DOUBLE,
          LOW        DOUBLE,
          CLOSE      DOUBLE,
          ADJ_CLOSE  DOUBLE,
          VOLUME     NUMBER(38,0),
          LOAD_TS    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
          CONSTRAINT PK_STOCK_PRICES PRIMARY KEY (SYMBOL, TRADE_DATE)
        );
        """)
    return f'Ensured "{schema}".STOCK_PRICES exists'

@task
def fetch_and_load_prices(**context):
    """Fetch from yfinance, stage into a TEMP table via executemany (DATE-safe), then upsert into target."""
    # Read config
    symbols_var = Variable.get("stock_symbols", default_var='["AAPL","MSFT","TSLA"]')
    try:
        symbols = json.loads(symbols_var)
        if not isinstance(symbols, list):
            raise ValueError
    except Exception:
        symbols = [s.strip() for s in symbols_var.split(",") if s.strip()]
    lookback_days = int(Variable.get("lookback_days", default_var="365"))
    schema = Variable.get("target_schema_raw", default_var="RAW")

    end_dt = datetime.now(timezone.utc).date()
    start_dt = end_dt - timedelta(days=lookback_days)

    df = _fetch_prices(symbols, start_dt.isoformat(), end_dt.isoformat())
    logging.info("ETL dataframe shape: %s", df.shape)

    if df.empty:
        return f"No rows fetched for symbols={symbols} in {start_dt}..{end_dt}"

    # Make sure TRADE_DATE is a Python date for clean binding
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce").dt.date
    if df["TRADE_DATE"].isna().any():
        # Should never happen, but guard anyway
        df = df[df["TRADE_DATE"].notna()].copy()

    target = f'"{schema}"."STOCK_PRICES"'
    tmp_name = f'STOCK_PRICES_LOAD_{int(datetime.now().timestamp())}'
    tmp_table = f'"{schema}"."{tmp_name}"'

    # Prepare row tuples for executemany
    rows = list(
        df[["TRADE_DATE", "SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]
        .itertuples(index=False, name=None)
    )

    with _sf_connect(schema) as conn, conn.cursor() as cur:
        try:
            # 1) Temp table to receive batch (same schema)
            cur.execute(f"CREATE TEMPORARY TABLE {tmp_table} LIKE {target};")

            # 2) Insert rows into temp table using executemany (DATE-safe)
            insert_sql = f"""
                INSERT INTO {tmp_table}
                (TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cur.executemany(insert_sql, rows)
            logging.info("Inserted %d rows into %s", len(rows), tmp_table)

            # 3) Upsert semantics: delete overlap on (SYMBOL, TRADE_DATE), then insert
            cur.execute(f"""
            DELETE FROM {target} t
            USING {tmp_table} s
            WHERE t.SYMBOL = s.SYMBOL
              AND t.TRADE_DATE = s.TRADE_DATE;
            """)
            cur.execute(f"""
            INSERT INTO {target} (TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME)
            SELECT TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME
            FROM {tmp_table};
            """)

            cur.execute(f"SELECT COUNT(*) FROM {target}")
            total = cur.fetchone()[0]
            return f"Loaded {len(rows)} rows. Total rows now in {schema}.STOCK_PRICES = {total}"
        finally:
            try:
                cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
            except Exception:
                pass


with DAG(
    dag_id="yfinance_etl",
    schedule="@daily",
    start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["etl", "yfinance", "snowflake"],
) as dag:
    t1 = ensure_objects()
    t2 = fetch_and_load_prices()
    t1 >> t2

