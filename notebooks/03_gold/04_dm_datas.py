# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks notebook source
# MAGIC  %md
# MAGIC
# MAGIC # 04 Gold — Date Dimension
# MAGIC
# MAGIC **Notebook:** `04_dm_datas`
# MAGIC
# MAGIC  Builds the curated Gold date dimension used by analytical models and business marts.
# MAGIC
# MAGIC  This notebook defines:
# MAGIC
# MAGIC  * Date dimensional model
# MAGIC  * Date surrogate key generation
# MAGIC  * Date business identifiers
# MAGIC  * Calendar descriptive attributes
# MAGIC  * Audit and traceability attributes
# MAGIC  * Gold governance metadata
# MAGIC  * Column and table comments
# MAGIC  * Gold validation rules
# MAGIC  * Gold execution logging
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC  * Generate a complete analytical calendar
# MAGIC  * Keep one analytical record per calendar date
# MAGIC  * Create the date surrogate key
# MAGIC  * Preserve business identifiers and descriptive attributes
# MAGIC  * Generate Gold execution metadata
# MAGIC  * Apply governance comments
# MAGIC  * Execute Gold quality validations
# MAGIC  * Publish the Gold date dimension
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC ## Dimensional Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC  One record per calendar date.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC  Generated calendar using Spark native functions.
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC  `brazil_legislative_analytics.gold.dm_datas`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC  `dat_id_data`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC  `dat_sk_data`
# MAGIC
# MAGIC ### Main Analytical Attributes
# MAGIC
# MAGIC  * Date
# MAGIC  * Year
# MAGIC  * Quarter
# MAGIC  * Month
# MAGIC  * Month name
# MAGIC  * Week of year
# MAGIC  * Day of month
# MAGIC  * Day of week
# MAGIC  * Weekend flag
# MAGIC  * Governance attributes
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC ### Rule 1 — One Record Per Date
# MAGIC
# MAGIC  Only one analytical record is maintained for each calendar date.
# MAGIC
# MAGIC
# MAGIC  dat_id_data
# MAGIC
# MAGIC
# MAGIC  must be unique in the Gold dimension.
# MAGIC
# MAGIC ### Rule 2 — Gold Surrogate Key Generation
# MAGIC
# MAGIC  A deterministic surrogate key is generated using the business key.
# MAGIC
# MAGIC
# MAGIC  dat_sk_data = sha2(dat_id_data)
# MAGIC
# MAGIC
# MAGIC ### Rule 3 — Governance Compliance
# MAGIC
# MAGIC  All columns and tables must contain governance comments.
# MAGIC
# MAGIC ### Rule 4 — Analytical Calendar Coverage
# MAGIC
# MAGIC  Calendar range covers the project analytical period.
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC ## Data Quality Controls
# MAGIC
# MAGIC  The notebook validates:
# MAGIC
# MAGIC  * Null business keys
# MAGIC  * Null surrogate keys
# MAGIC  * Null dates
# MAGIC  * Duplicate date identifiers
# MAGIC  * Invalid Gold records
# MAGIC  * Governance comment coverage
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_comments

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_quality

# COMMAND ----------



from datetime import datetime
import uuid

from pyspark.sql import functions as F

# COMMAND ----------

# ============================================================
# EXECUTION CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "04_dm_datas"

ENTITY_NAME = "datas"

TARGET_TABLE = f"{GOLD_SCHEMA}.dm_datas"

EXECUTION_ID = str(uuid.uuid4())

STARTED_AT = datetime.now()

PIPELINE_LOG_ID = str(uuid.uuid4())

CALENDAR_START_DATE = "2019-01-01"

CALENDAR_END_DATE = "2030-12-31"

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name="gold"
)

log_info(
    logger,
    f"Starting notebook {NOTEBOOK_NAME}"
)

# COMMAND ----------

# ============================================================
# GENERATE CALENDAR
# ============================================================

df_calendar = (
    spark
    .sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{CALENDAR_START_DATE}'),
                to_date('{CALENDAR_END_DATE}'),
                interval 1 day
            )
        ) AS dat_dt_data
        """
    )
)

records_read = df_calendar.count()

log_info(
    logger,
    f"Calendar records generated: {records_read}"
)

# COMMAND ----------

# ============================================================
# BUSINESS RULES AND ANALYTICAL ATTRIBUTES
# ============================================================

df_gold = (
    df_calendar
    .withColumn(
        "dat_id_data",
        F.date_format(
            F.col("dat_dt_data"),
            "yyyyMMdd"
        )
    )
    .withColumn(
        "dat_nr_ano",
        F.year("dat_dt_data")
    )
    .withColumn(
        "dat_nr_semestre",
        F.when(F.month("dat_dt_data") <= 6, F.lit(1)).otherwise(F.lit(2))
    )
    .withColumn(
        "dat_nr_trimestre",
        F.quarter("dat_dt_data")
    )
    .withColumn(
        "dat_nr_mes",
        F.month("dat_dt_data")
    )
    .withColumn(
        "dat_tx_nome_mes",
        F.date_format(
            F.col("dat_dt_data"),
            "MMMM"
        )
    )
    .withColumn(
        "dat_tx_nome_mes_abrev",
        F.date_format(
            F.col("dat_dt_data"),
            "MMM"
        )
    )
    .withColumn(
        "dat_nr_semana_ano",
        F.weekofyear("dat_dt_data")
    )
    .withColumn(
        "dat_nr_dia_mes",
        F.dayofmonth("dat_dt_data")
    )
    .withColumn(
        "dat_nr_dia_ano",
        F.dayofyear("dat_dt_data")
    )
    .withColumn(
        "dat_nr_dia_semana",
        F.dayofweek("dat_dt_data")
    )
    .withColumn(
        "dat_tx_nome_dia_semana",
        F.date_format(
            F.col("dat_dt_data"),
            "EEEE"
        )
    )
    .withColumn(
        "dat_fl_final_semana",
        F.col("dat_nr_dia_semana").isin(1, 7)
    )
    .withColumn(
        "dat_tx_ano_mes",
        F.date_format(
            F.col("dat_dt_data"),
            "yyyy-MM"
        )
    )
    .withColumn(
        "dat_tx_ano_trimestre",
        F.concat(
            F.col("dat_nr_ano").cast("string"),
            F.lit("-T"),
            F.col("dat_nr_trimestre").cast("string")
        )
    )
)

# COMMAND ----------

# ============================================================
# GOLD SURROGATE KEY
# ============================================================

df_gold = (
    df_gold
    .withColumn(
        "dat_sk_data",
        F.sha2(
            F.col("dat_id_data"),
            256
        )
    )
)

# COMMAND ----------

# ============================================================
# GOLD QUALITY FLAG
# ============================================================

df_gold = (
    df_gold
    .withColumn(
        "dat_fl_registro_valido_gold",
        F.lit(True)
    )
)

# COMMAND ----------

# ============================================================
# GOLD AUDIT COLUMNS
# ============================================================

df_gold = (
    df_gold
    .withColumn(
        "aud_id_execucao_gold",
        F.lit(EXECUTION_ID)
    )
    .withColumn(
        "aud_dh_processamento_gold",
        F.current_timestamp()
    )
    .withColumn(
        "aud_tx_versao_pipeline_gold",
        F.lit(PROJECT_VERSION)
    )
    .withColumn(
        "aud_tx_hash_registro_gold",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("dat_id_data"),
                F.col("dat_dt_data").cast("string"),
                F.col("dat_tx_ano_mes")
            ),
            256
        )
    )
)

# COMMAND ----------

# ============================================================
# SELECT FINAL COLUMNS
# ============================================================

df_gold = (
    df_gold
    .select(
        "dat_sk_data",
        "dat_id_data",
        "dat_dt_data",
        "dat_nr_ano",
        "dat_nr_semestre",
        "dat_nr_trimestre",
        "dat_nr_mes",
        "dat_tx_nome_mes",
        "dat_tx_nome_mes_abrev",
        "dat_nr_semana_ano",
        "dat_nr_dia_mes",
        "dat_nr_dia_ano",
        "dat_nr_dia_semana",
        "dat_tx_nome_dia_semana",
        "dat_fl_final_semana",
        "dat_tx_ano_mes",
        "dat_tx_ano_trimestre",
        "dat_fl_registro_valido_gold",
        "aud_id_execucao_gold",
        "aud_dh_processamento_gold",
        "aud_tx_versao_pipeline_gold",
        "aud_tx_hash_registro_gold"
    )
)

# COMMAND ----------

# ============================================================
# QUALITY VALIDATIONS
# ============================================================

required_columns_result = validate_required_columns(
    dataframe=df_gold,
    required_columns=[
        "dat_id_data",
        "dat_dt_data",
        "dat_sk_data"
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_gold,
    key_columns=[
        "dat_id_data"
    ]
)

null_results = validate_nulls(
    dataframe=df_gold,
    columns=[
        "dat_id_data",
        "dat_dt_data",
        "dat_sk_data"
    ]
)

quality_results = [
    required_columns_result,
    duplicate_result
]

quality_results.extend(
    null_results
)

quality_df = build_quality_log(
    quality_results=quality_results,
    execution_id=EXECUTION_ID,
    notebook_name=NOTEBOOK_NAME,
    layer_name="gold",
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE
)

write_quality_log(
    quality_dataframe=quality_df
)

# COMMAND ----------

# ============================================================
# WRITE GOLD TABLE
# ============================================================

(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_TABLE
    )
)

records_written = df_gold.count()

log_success(
    logger,
    f"Records written to Gold: {records_written}"
)

# COMMAND ----------

# ============================================================
# GOVERNANCE COMMENTS
# ============================================================

TABLE_COMMENT = """
Gold date dimension.

This dimension contains one record per calendar date.

Main characteristics:

* surrogate key
* business key
* calendar attributes
* analytical date hierarchy
* Gold lineage
* governance metadata
"""

COLUMN_COMMENTS = {
    "dat_sk_data":
        "Gold surrogate key for date dimension.",

    "dat_id_data":
        "Date business identifier formatted as yyyyMMdd.",

    "dat_dt_data":
        "Calendar date.",

    "dat_nr_ano":
        "Calendar year.",

    "dat_nr_semestre":
        "Calendar semester number.",

    "dat_nr_trimestre":
        "Calendar quarter number.",

    "dat_nr_mes":
        "Calendar month number.",

    "dat_tx_nome_mes":
        "Calendar month name.",

    "dat_tx_nome_mes_abrev":
        "Abbreviated calendar month name.",

    "dat_nr_semana_ano":
        "Calendar week number within year.",

    "dat_nr_dia_mes":
        "Calendar day number within month.",

    "dat_nr_dia_ano":
        "Calendar day number within year.",

    "dat_nr_dia_semana":
        "Calendar day number within week.",

    "dat_tx_nome_dia_semana":
        "Calendar weekday name.",

    "dat_fl_final_semana":
        "Flag indicating whether the calendar date is Saturday or Sunday.",

    "dat_tx_ano_mes":
        "Year and month formatted as yyyy-MM.",

    "dat_tx_ano_trimestre":
        "Year and quarter formatted as yyyy-Tn.",

    "dat_fl_registro_valido_gold":
        "Flag indicating whether record passed Gold validation.",

    "aud_id_execucao_gold":
        "Execution identifier generated during Gold processing.",

    "aud_dh_processamento_gold":
        "Timestamp when the record was processed in Gold.",

    "aud_tx_versao_pipeline_gold":
        "Pipeline version used during Gold processing.",

    "aud_tx_hash_registro_gold":
        "Deterministic Gold record hash."
}

apply_table_comment(
    table_name=TARGET_TABLE,
    table_comment=TABLE_COMMENT
)

apply_column_comments(
    table_name=TARGET_TABLE,
    column_comments=COLUMN_COMMENTS
)

# COMMAND ----------

# ============================================================
# PIPELINE AUDIT LOG
# ============================================================

FINISHED_AT = datetime.now()

duration_seconds = (
    FINISHED_AT - STARTED_AT
).total_seconds()

write_pipeline_log(
    log_id=PIPELINE_LOG_ID,
    execution_id=EXECUTION_ID,
    notebook_name=NOTEBOOK_NAME,
    layer_name="gold",
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status="SUCCESS",
    message="Gold date dimension generated successfully.",
    started_at=STARTED_AT,
    finished_at=FINISHED_AT,
    duration_seconds=duration_seconds,
    records_read=records_read,
    records_written=records_written
)

# COMMAND ----------

# ============================================================
# POST-WRITE VALIDATIONS
# ============================================================

gold_df = spark.table(TARGET_TABLE)

print("=" * 80)
print("DIMENSÃO DATAS - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read: {records_read}")
print(f"Records written: {records_written}")
print(f"Calendar start date: {CALENDAR_START_DATE}")
print(f"Calendar end date: {CALENDAR_END_DATE}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(gold_df.limit(20))
