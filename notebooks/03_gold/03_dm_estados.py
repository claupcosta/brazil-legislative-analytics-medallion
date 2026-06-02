# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold — Estados Dimension
# MAGIC
# MAGIC **Notebook:** `03_dm_estados`
# MAGIC
# MAGIC Builds the curated Gold state dimension used by analytical models and business marts.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * State dimensional model
# MAGIC * State surrogate key generation
# MAGIC * State descriptive attributes
# MAGIC * Geographic attributes
# MAGIC * Audit and traceability attributes
# MAGIC * Gold governance metadata
# MAGIC * Column and table comments
# MAGIC * Gold validation rules
# MAGIC * Gold execution logging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC * Read validated state records from Silver
# MAGIC * Keep one analytical record per Brazilian federative unit
# MAGIC * Create the state surrogate key
# MAGIC * Preserve business identifiers and descriptive attributes
# MAGIC * Preserve audit and traceability information
# MAGIC * Generate Gold execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Gold quality validations
# MAGIC * Publish the Gold state dimension
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dimensional Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per Brazilian federative unit (UF).
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC `brazil_legislative_analytics.silver.slv_estados`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.gold.dm_estados`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `est_id_estado`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC `est_sk_estado`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC Rule 1:
# MAGIC
# MAGIC Only Silver approved records are eligible for Gold.
# MAGIC
# MAGIC Rule 2:
# MAGIC
# MAGIC One analytical record per federative unit.
# MAGIC
# MAGIC Rule 3:
# MAGIC
# MAGIC Preserve governance and lineage information.
# MAGIC
# MAGIC Rule 4:
# MAGIC
# MAGIC All Gold objects must contain governance comments.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Controls
# MAGIC
# MAGIC Validates:
# MAGIC
# MAGIC * Null surrogate keys
# MAGIC * Null business keys
# MAGIC * Null UF acronym
# MAGIC * Duplicate states
# MAGIC * Invalid Gold records
# MAGIC
# MAGIC Execution is interrupted when critical validations fail.

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

# ============================================================
# EXECUTION CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "03_dm_estados"

ENTITY_NAME = "estados"

SOURCE_TABLE = f"{SILVER_SCHEMA}.slv_estados"

TARGET_TABLE = f"{GOLD_SCHEMA}.dm_estados"

EXECUTION_ID = str(uuid.uuid4())

STARTED_AT = datetime.now()

PIPELINE_LOG_ID = str(uuid.uuid4())

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
# READ SILVER
# ============================================================

df_silver = spark.table(SOURCE_TABLE)

records_read = df_silver.count()

log_info(
    logger,
    f"Records read from Silver: {records_read}"
)

# COMMAND ----------

# ============================================================
# BUSINESS RULES
# ============================================================

df_gold = (

    df_silver

    .filter(
        F.col("est_fl_registro_valido_silver") == True
    )

)

# COMMAND ----------

# ============================================================
# GOLD SURROGATE KEY
# ============================================================

df_gold = (

    df_gold

    .withColumn(
        "est_sk_estado",
        F.sha2(
            F.col("est_id_estado"),
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
        "est_fl_registro_valido_gold",
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
                F.col("est_id_estado"),
                F.col("est_tx_sigla_uf"),
                F.col("est_tx_nome"),
                F.col("est_tx_regiao"),
                F.col("est_tx_pais"),
                F.col("est_tx_codigo_pais")
            ),
            256
        )
    )

)

# COMMAND ----------

# ============================================================
# QUALITY VALIDATIONS
# ============================================================

required_columns_result = validate_required_columns(
    dataframe=df_gold,
    required_columns=[
        "est_id_estado",
        "est_tx_sigla_uf",
        "est_tx_nome",
        "est_sk_estado"
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_gold,
    key_columns=[
        "est_id_estado"
    ]
)

null_results = validate_nulls(
    dataframe=df_gold,
    columns=[
        "est_id_estado",
        "est_tx_sigla_uf",
        "est_tx_nome",
        "est_sk_estado"
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
Gold Brazilian states dimension.

This dimension contains one record per Brazilian state.

Main characteristics:

* surrogate key
* business key
* analytical attributes
* Silver lineage
* Gold lineage
* governance metadata
"""

COLUMN_COMMENTS = {

    "est_sk_estado":
        "Gold surrogate key for Brazilian state dimension.",

    "est_id_estado":
        "Deterministic Brazilian state identifier generated from the state acronym.",

    "est_tx_sigla_uf":
        "Standardized Brazilian state acronym.",

    "est_tx_nome":
        "Brazilian state name.",

    "est_tx_regiao":
        "Brazilian geographic region name.",

    "est_tx_pais":
        "Country name associated with the state.",

    "est_tx_codigo_pais":
        "Country code associated with the state.",

    "est_fl_registro_valido_gold":
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
    message="Gold Brazilian states dimension generated successfully.",
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
print("DIMENSÃO ESTADOS - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read: {records_read}")
print(f"Records written: {records_written}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(gold_df.limit(20))
