# dags/dag_ml_forecast_tf.py — TaskFlow DAG2 (fixed: no SP.TS, uses TRADE_DATE)
from datetime import datetime, timezone
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import snowflake.connector

SNOWFLAKE_CONN_ID = "snowflake_catfish"

# Schemas / tables (uppercase = unquoted identifiers in Snowflake)
RAW, MODEL, ANALYTICS = "RAW", "MODEL", "ANALYTICS"
RAW_STOCK_PRICES = f"{RAW}.STOCK_PRICES"           # columns include: TRADE_DATE (DATE), SYMBOL, CLOSE, ...
MODEL_FORECASTS  = f"{MODEL}.FORECASTS"            # this DAG manages it
ANALYTICS_FINAL  = f"{ANALYTICS}.FINAL_PRICES_FORECAST"

def _sf_connect(default_schema=None):
    c = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = c.extra_dejson or {}
    kw = {
        "account":   extra.get("account"),
        "user":      c.login,
        "password":  c.password,
        "warehouse": extra.get("warehouse"),
        "database":  extra.get("database"),
        "schema":    default_schema or extra.get("schema"),
        "role":      extra.get("role"),
        "client_session_keep_alive": True,
    }
    return snowflake.connector.connect(**{k: v for k, v in kw.items() if v is not None})

@dag(
    dag_id="ml_forecast_tf",
    schedule="@daily",
    start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ml", "forecast", "snowflake"],
)
def ml_forecast_tf():

    @task
    def ensure_objects():
        conn = _sf_connect(MODEL)
        cur = conn.cursor()
        try:
            conn.autocommit(False)
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {MODEL}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {ANALYTICS}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {MODEL_FORECASTS} (
                  SYMBOL STRING NOT NULL,
                  TS DATE NOT NULL,                       -- forecast horizon timestamps (date-granularity)
                  PREDICTED_CLOSE FLOAT NOT NULL,
                  MODEL_NAME STRING NOT NULL,
                  TRAINED_AT TIMESTAMP_NTZ NOT NULL,
                  HORIZON_D NUMBER(5,0) NOT NULL,
                  LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                  CONSTRAINT PK_FORECASTS PRIMARY KEY (SYMBOL, TS, MODEL_NAME)
                )
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ANALYTICS_FINAL} (
                  SYMBOL STRING NOT NULL,
                  TS DATE NOT NULL,                       -- time axis (actuals + forecasts)
                  CLOSE FLOAT,
                  SOURCE STRING NOT NULL,                 -- 'ACTUAL' or 'FORECAST'
                  MODEL_NAME STRING,
                  LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                  CONSTRAINT PK_FINAL PRIMARY KEY (SYMBOL, TS, SOURCE)
                )
            """)
            conn.commit()
        except Exception:
            conn.rollback(); raise
        finally:
            cur.close(); conn.close()

    @task
    def train_model_and_forecast():
        syms_raw   = Variable.get("stock_symbols", default_var='["AAPL","MSFT","TSLA"]')
        lookback   = int(Variable.get("lookback_days", "365"))
        horizon    = int(Variable.get("forecast_horizon_days", "14"))
        syms_json  = syms_raw.replace("'", "''")  # escape single quotes inside JSON string

        conn = _sf_connect(MODEL)
        cur = conn.cursor()
        try:
            # ---------- Phase 1: setup (transactional) ----------
            conn.autocommit(False)
            cur.execute(f"USE SCHEMA {MODEL}")

            # Symbols temp table from JSON variable
            cur.execute(f"""
                CREATE OR REPLACE TEMP TABLE SYMBOLS AS
                SELECT value::string AS SYMBOL
                FROM TABLE(FLATTEN(input => PARSE_JSON('{syms_json}')))
            """)

            # Training data view:
            # Use RAW.STOCK_PRICES.TRADE_DATE (DATE) and alias to TS (TIMESTAMP_NTZ) for Snowflake ML.
            # Also restrict by lookback days using CURRENT_DATE() (not timestamp).
            cur.execute(f"""
                CREATE OR REPLACE TEMP VIEW TRAINING_DATA AS
                SELECT
                  TO_VARIANT(sp.SYMBOL)              AS SERIES,     -- series key col
                  sp.TRADE_DATE::TIMESTAMP_NTZ       AS TS,         -- << alias DATE → TS for ML
                  sp.CLOSE                           AS CLOSE
                FROM {RAW_STOCK_PRICES} sp
                JOIN SYMBOLS s ON s.SYMBOL = sp.SYMBOL
                WHERE sp.TRADE_DATE >= DATEADD('day', -{lookback}, CURRENT_DATE())
                  AND sp.CLOSE IS NOT NULL
            """)

            # Guard: ensure non-empty training input
            cur.execute("SELECT COUNT(*) FROM TRAINING_DATA")
            if cur.fetchone()[0] == 0:
                conn.rollback()
                from airflow.exceptions import AirflowSkipException
                raise AirflowSkipException("Training skipped: TRAINING_DATA is empty.")

            conn.commit()  # end user txn before ML creation/inference

            # ---------- Phase 2: ML (autocommit) ----------
            conn.autocommit(True)

            # Create/replace a named ML forecast object
            cur.execute("""
                CREATE OR REPLACE SNOWFLAKE.ML.FORECAST PRICE_FORECASTER (
                  INPUT_DATA        => SYSTEM$QUERY_REFERENCE($$ SELECT SERIES, TS, CLOSE FROM TRAINING_DATA $$),
                  SERIES_COLNAME    => 'SERIES',
                  TIMESTAMP_COLNAME => 'TS',
                  TARGET_COLNAME    => 'CLOSE',
                  CONFIG_OBJECT     => PARSE_JSON('{"method":"fast","on_error":"skip"}')
                )
            """)

            # Forecast out horizon days; cast TS to DATE for our MODEL_FORECASTS table
            cur.execute(f"""
                CREATE OR REPLACE TEMP TABLE TMP_FC AS
                SELECT
                  SERIES::STRING              AS SYMBOL,
                  CAST(TS AS DATE)            AS TS,
                  FORECAST                    AS PREDICTED_CLOSE,
                  'SNOWFLAKE_ML'              AS MODEL_NAME,
                  CURRENT_TIMESTAMP()         AS TRAINED_AT,
                  {horizon}::NUMBER           AS HORIZON_D
                FROM TABLE(PRICE_FORECASTER!FORECAST(FORECASTING_PERIODS => {horizon}))
            """)

            # Guard: ensure we actually got forecasts
            cur.execute("SELECT COUNT(*) FROM TMP_FC")
            if cur.fetchone()[0] == 0:
                from airflow.exceptions import AirflowSkipException
                raise AirflowSkipException("Forecast step produced 0 rows.")

            # ---------- Phase 3: upsert into MODEL.FORECASTS (transactional) ----------
            conn.autocommit(False)
            cur.execute(f"""
                MERGE INTO {MODEL_FORECASTS} t
                USING TMP_FC s
                ON  t.SYMBOL = s.SYMBOL
                AND t.TS     = s.TS
                AND t.MODEL_NAME = s.MODEL_NAME
                WHEN MATCHED THEN UPDATE SET
                  PREDICTED_CLOSE = s.PREDICTED_CLOSE,
                  TRAINED_AT      = s.TRAINED_AT,
                  HORIZON_D       = s.HORIZON_D,
                  LOAD_TS         = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                  (SYMBOL, TS, PREDICTED_CLOSE, MODEL_NAME, TRAINED_AT, HORIZON_D)
                  VALUES (s.SYMBOL, s.TS, s.PREDICTED_CLOSE, s.MODEL_NAME, s.TRAINED_AT, s.HORIZON_D)
            """)
            conn.commit()
        except Exception:
            try: conn.rollback()
            except Exception: pass
            raise
        finally:
            cur.close(); conn.close()

    @task
    def build_final_union():
        # limit to the same symbol set the model used
        syms_raw  = Variable.get("stock_symbols", default_var='["AAPL","MSFT","TSLA"]')
        syms_json = syms_raw.replace("'", "''")

        conn = _sf_connect(ANALYTICS)
        cur = conn.cursor()
        try:
            conn.autocommit(False)
            cur.execute(f"USE SCHEMA {ANALYTICS}")

            cur.execute(f"""
                CREATE OR REPLACE TEMP TABLE SYMBOLS AS
                SELECT value::string AS SYMBOL
                FROM TABLE(FLATTEN(input => PARSE_JSON('{syms_json}')))
            """)

            # Rebuild final union (actuals + forecasts)
            cur.execute(f"TRUNCATE TABLE {ANALYTICS_FINAL}")

            # ACTUALS from RAW.STOCK_PRICES: use TRADE_DATE (DATE) directly as TS
            cur.execute(f"""
                INSERT INTO {ANALYTICS_FINAL} (SYMBOL, TS, CLOSE, SOURCE, MODEL_NAME)
                SELECT sp.SYMBOL,
                       sp.TRADE_DATE AS TS,
                       sp.CLOSE,
                       'ACTUAL'      AS SOURCE,
                       NULL          AS MODEL_NAME
                FROM {RAW_STOCK_PRICES} sp
                JOIN SYMBOLS s ON s.SYMBOL = sp.SYMBOL
            """)

            # FORECASTS from MODEL.FORECASTS (TS already DATE)
            cur.execute(f"""
                INSERT INTO {ANALYTICS_FINAL} (SYMBOL, TS, CLOSE, SOURCE, MODEL_NAME)
                SELECT f.SYMBOL,
                       f.TS,
                       f.PREDICTED_CLOSE AS CLOSE,
                       'FORECAST'        AS SOURCE,
                       f.MODEL_NAME
                FROM {MODEL_FORECASTS} f
                JOIN SYMBOLS s ON s.SYMBOL = f.SYMBOL
            """)

            conn.commit()
        except Exception:
            try: conn.rollback()
            except Exception: pass
            raise
        finally:
            cur.close(); conn.close()

    # TaskFlow wiring
    e = ensure_objects()
    t = train_model_and_forecast()
    u = build_final_union()
    e >> t >> u

ml_forecast_tf()
