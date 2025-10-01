# Airflow DAG #2 — Snowflake ML Forecast → Snowflake (+ UNION to final)
# All secrets (account/user/password/warehouse/database/role) are in the Airflow Connection: snowflake_catfish
# Schemas come from Variables. No DB/WH hardcoded; no USE DATABASE/WAREHOUSE.

from datetime import datetime, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = "snowflake_catfish"

SCHEMA_RAW = Variable.get("target_schema_raw", default_var="RAW")
SCHEMA_MODEL = Variable.get("target_schema_model", default_var="MODEL")
SCHEMA_AN = Variable.get("target_schema_analytics", default_var="ANALYTICS")

default_args = {"depends_on_past": False, "retries": 1}

with DAG(
    dag_id="ml_forecast",
    schedule="@daily", # Changed schedule_interval to schedule
    start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["ml", "forecast", "snowflake"],
) as dag:

    # 1) Ensure schemas & tables (transactional)
    ensure_objects = SnowflakeOperator(
        task_id="ensure_model_and_analytics",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        BEGIN;

          -- rely on connection's current database/warehouse
          CREATE SCHEMA IF NOT EXISTS {SCHEMA_MODEL};
          CREATE SCHEMA IF NOT EXISTS {SCHEMA_AN};

          CREATE TABLE IF NOT EXISTS {SCHEMA_MODEL}.FORECASTS (
            SYMBOL STRING NOT NULL,
            TS DATE NOT NULL,
            PREDICTED_CLOSE FLOAT NOT NULL,
            MODEL_NAME STRING NOT NULL,
            TRAINED_AT TIMESTAMP_NTZ NOT NULL,
            HORIZON_D NUMBER(5,0) NOT NULL,
            LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            CONSTRAINT PK_FORECASTS PRIMARY KEY (SYMBOL, TS, MODEL_NAME)
          );

          CREATE TABLE IF NOT EXISTS {SCHEMA_AN}.FINAL_PRICES_FORECAST (
            SYMBOL STRING NOT NULL,
            TS DATE NOT NULL,
            CLOSE FLOAT,
            SOURCE STRING NOT NULL,     -- 'ACTUAL' or 'FORECAST'
            MODEL_NAME STRING,          -- null for ACTUAL
            LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            CONSTRAINT PK_FINAL PRIMARY KEY (SYMBOL, TS, SOURCE)
          );

        COMMIT;
        """,
    )

    # 2) Train model with SNOWFLAKE.ML.FORECAST and write predictions (transactional)
    train_model_and_forecast = SnowflakeOperator(
        task_id="train_model_and_forecast",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        BEGIN;

          -- Set current schema for model object
          USE SCHEMA {SCHEMA_MODEL};

          -- Flatten symbol list from Airflow Variable
          WITH symbols AS (
            SELECT value::string AS symbol
            FROM TABLE(FLATTEN(input => PARSE_JSON('{{{{ var.value.stock_symbols }}}}')))
          ),
          training_data AS (
            SELECT
              TO_VARIANT(sp.SYMBOL) AS SERIES,
              sp.TS,
              sp.CLOSE
            FROM {SCHEMA_RAW}.STOCK_PRICES sp
            JOIN symbols s ON s.symbol = sp.SYMBOL
            WHERE sp.TS >= DATEADD('day', -{{{{ var.value.lookback_days | default('365', true) }}}}, CURRENT_TIMESTAMP())
          )

          -- Create/replace multi-series forecast model
          CREATE OR REPLACE SNOWFLAKE.ML.FORECAST PRICE_FORECASTER (
            INPUT_DATA => SYSTEM$QUERY_REFERENCE($$
              SELECT SERIES, TS, CLOSE FROM training_data
            $$),
            SERIES_COLNAME => 'SERIES',
            TIMESTAMP_COLNAME => 'TS',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {{
              'method': 'fast',
              'on_error': 'skip'
            }}
          );

          -- Forecast and stage into temp table
          CREATE OR REPLACE TEMP TABLE TMP_FC AS
          SELECT
            SERIES::STRING AS SYMBOL,
            CAST(TS AS DATE) AS TS,
            FORECAST AS PREDICTED_CLOSE,
            'SNOWFLAKE_ML' AS MODEL_NAME,
            CURRENT_TIMESTAMP() AS TRAINED_AT,
            {{{{ var.value.forecast_horizon_days | default('14', true) }}}}::NUMBER AS HORIZON_D
          FROM TABLE(PRICE_FORECASTER!FORECAST(
            FORECASTING_PERIODS => {{{{ var.value.forecast_horizon_days | default('14', true) }}}}
          ));

          -- Upsert forecasts
          MERGE INTO {SCHEMA_MODEL}.FORECASTS t
          USING TMP_FC s
          ON  t.SYMBOL = s.SYMBOL
          AND t.TS = s.TS
          AND t.MODEL_NAME = s.MODEL_NAME
          WHEN MATCHED THEN UPDATE SET
            PREDICTED_CLOSE = s.PREDICTED_CLOSE,
            TRAINED_AT = s.TRAINED_AT,
            HORIZON_D = s.HORIZON_D,
            LOAD_TS = CURRENT_TIMESTAMP()
          WHEN NOT MATCHED THEN INSERT
            (SYMBOL, TS, PREDICTED_CLOSE, MODEL_NAME, TRAINED_AT, HORIZON_D)
            VALUES (s.SYMBOL, s.TS, s.PREDICTED_CLOSE, s.MODEL_NAME, s.TRAINED_AT, s.HORIZON_D);

        COMMIT;
        """,
    )

    # 3) Build FINAL union (transactional)
    build_final_union = SnowflakeOperator(
        task_id="build_final_union",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        BEGIN;

          USE SCHEMA {SCHEMA_AN};

          WITH symbols AS (
            SELECT value::string AS symbol
            FROM TABLE(FLATTEN(input => PARSE_JSON('{{{{ var.value.stock_symbols }}}}')))
          )

          TRUNCATE TABLE {SCHEMA_AN}.FINAL_PRICES_FORECAST;

          INSERT INTO {SCHEMA_AN}.FINAL_PRICES_FORECAST (SYMBOL, TS, CLOSE, SOURCE, MODEL_NAME)
          SELECT
            sp.SYMBOL,
            CAST(sp.TS AS DATE) AS TS,
            sp.CLOSE,
            'ACTUAL' AS SOURCE,
            NULL AS MODEL_NAME
          FROM {SCHEMA_RAW}.STOCK_PRICES sp
          JOIN symbols s ON s.symbol = sp.SYMBOL;

          INSERT INTO {SCHEMA_AN}.FINAL_PRICES_FORECAST (SYMBOL, TS, CLOSE, SOURCE, MODEL_NAME)
          SELECT
            f.SYMBOL,
            f.TS,
            f.PREDICTED_CLOSE AS CLOSE,
            'FORECAST' AS SOURCE,
            f.MODEL_NAME
          FROM {SCHEMA_MODEL}.FORECASTS f
          JOIN symbols s ON s.symbol = f.SYMBOL;

        COMMIT;
        """,
    )

    ensure_objects >> train_model_and_forecast >> build_final_union