# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC # 05 Gold — Parliamentary Fronts Dimension
# MAGIC
# MAGIC **Notebook:** `05_dm_frentes`
# MAGIC
# MAGIC Builds the curated Gold parliamentary fronts dimension used by analytical models and business marts.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Parliamentary front dimensional model
# MAGIC * Parliamentary front surrogate key generation
# MAGIC * Parliamentary front business identifiers
# MAGIC * Parliamentary front descriptive attributes
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
# MAGIC * Read validated parliamentary front records from Silver
# MAGIC * Keep one analytical record per parliamentary front
# MAGIC * Create the parliamentary front surrogate key
# MAGIC * Preserve business identifiers and descriptive attributes
# MAGIC * Preserve audit and traceability information
# MAGIC * Generate Gold execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Gold quality validations
# MAGIC * Publish the Gold parliamentary fronts dimension
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dimensional Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per parliamentary front.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC `brazil_legislative_analytics.silver.slv_frentes`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.gold.dm_frentes`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `frn_id_frente`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC `frn_sk_frente`
# MAGIC
# MAGIC ### Main Analytical Attributes
# MAGIC
# MAGIC * Parliamentary front title
# MAGIC * Parliamentary front URI
# MAGIC * Legislature
# MAGIC * Registration status
# MAGIC * Governance attributes
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

# ============================================================
# EXECUTION CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "05_dm_frentes"

ENTITY_NAME = "frentes"

SOURCE_TABLE = f"{SILVER_SCHEMA}.slv_frentes"

TARGET_TABLE = f"{GOLD_SCHEMA}.dm_frentes"

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
        F.col("frn_fl_registro_valido_silver") == True
    )
)

# COMMAND ----------

# ============================================================
# GOLD SURROGATE KEY
# ============================================================

df_gold = (
    df_gold
    .withColumn(
        "frn_sk_frente",
        F.sha2(
            F.col("frn_id_frente").cast("string"),
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
        "frn_fl_registro_valido_gold",
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
                F.col("frn_id_frente").cast("string"),
                F.col("frn_tx_titulo").cast("string"),
                F.col("leg_id_legislatura").cast("string")
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
        "frn_id_frente",
        "frn_tx_titulo",
        "frn_sk_frente"
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_gold,
    key_columns=[
        "frn_id_frente"
    ]
)

null_results = validate_nulls(
    dataframe=df_gold,
    columns=[
        "frn_id_frente",
        "frn_tx_titulo",
        "frn_sk_frente"
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
Gold parliamentary fronts dimension.

This dimension contains one record per Brazilian parliamentary front.

Main characteristics:

* surrogate key
* business key
* analytical attributes
* Silver lineage
* Gold lineage
* governance metadata
"""

COLUMN_COMMENTS = {
    "frn_sk_frente":
        "Gold surrogate key for parliamentary front dimension.",

    "frn_id_frente":
        "Business identifier of the parliamentary front from the source system.",

    "frn_tx_titulo":
        "Standardized parliamentary front title.",

    "frn_tx_uri":
        "Parliamentary front URI from the source system.",

    "leg_id_legislatura":
        "Legislature identifier associated with the parliamentary front.",

    "frn_fl_registro_valido_silver":
        "Flag indicating whether record passed Silver validation.",

    "frn_fl_registro_valido_gold":
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

existing_columns = set(spark.table(TARGET_TABLE).columns)

COLUMN_COMMENTS = {
    column_name: column_comment
    for column_name, column_comment in COLUMN_COMMENTS.items()
    if column_name in existing_columns
}

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
    message="Gold parliamentary fronts dimension generated successfully.",
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
print("DIMENSÃO FRENTES - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read: {records_read}")
print(f"Records written: {records_written}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(gold_df.limit(20))
