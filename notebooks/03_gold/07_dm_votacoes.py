# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 07 Gold — Voting Dimension
# MAGIC
# MAGIC **Notebook:** `07_dm_votacoes`
# MAGIC
# MAGIC Builds the curated Gold voting dimension used by analytical models and business marts.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Legislative voting dimensional model
# MAGIC * Legislative voting surrogate key generation
# MAGIC * Voting descriptive attributes
# MAGIC * Voting result attributes
# MAGIC * Organizational attributes
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
# MAGIC * Read validated legislative voting records from Silver
# MAGIC * Keep one analytical record per legislative voting event
# MAGIC * Create the legislative voting surrogate key
# MAGIC * Preserve business identifiers and descriptive attributes
# MAGIC * Preserve audit and traceability information
# MAGIC * Generate Gold execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Gold quality validations
# MAGIC * Publish the Gold voting dimension
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dimensional Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per legislative voting event.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC `brazil_legislative_analytics.silver.slv_votacoes`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.gold.dm_votacoes`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `vot_id_votacao`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC `vot_sk_votacao`
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
# MAGIC One analytical record per legislative voting event.
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
# MAGIC * Null voting identifiers
# MAGIC * Duplicate voting records
# MAGIC * Invalid Gold records
# MAGIC
# MAGIC Execution is interrupted when critical validations fail.
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

NOTEBOOK_NAME = "07_dm_votacoes"

ENTITY_NAME = "votacoes"

SOURCE_TABLE = f"{SILVER_SCHEMA}.slv_votacoes"

TARGET_TABLE = f"{GOLD_SCHEMA}.dm_votacoes"

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
        F.col("vot_fl_registro_valido_silver") == True
    )
)

# COMMAND ----------

# ============================================================
# GOLD SURROGATE KEY
# ============================================================

df_gold = (
    df_gold
    .withColumn(
        "vot_sk_votacao",
        F.sha2(
            F.col("vot_id_votacao").cast("string"),
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
        "vot_fl_registro_valido_gold",
        F.lit(True)
    )
)

# COMMAND ----------

# ============================================================
# GOLD AUDIT COLUMNS
# ============================================================

hash_columns = [
    F.col(column_name).cast("string")
    for column_name in [
        "vot_id_votacao",
        "vot_dt_votacao",
        "vot_tx_descricao",
        "vot_tx_resultado",
        "vot_tx_sigla_orgao",
        "prop_id_proposicao"
    ]
    if column_name in df_gold.columns
]

if not hash_columns:
    hash_columns = [
        F.col("vot_id_votacao").cast("string")
    ]

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
                *hash_columns
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
        "vot_id_votacao",
        "vot_sk_votacao"
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_gold,
    key_columns=[
        "vot_id_votacao"
    ]
)

null_results = validate_nulls(
    dataframe=df_gold,
    columns=[
        "vot_id_votacao",
        "vot_sk_votacao"
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
Gold legislative voting dimension.

This dimension contains one record per legislative voting event.

Main characteristics:

* surrogate key
* business key
* analytical attributes
* Silver lineage
* Gold lineage
* governance metadata
"""

COLUMN_COMMENTS = {
    "vot_sk_votacao":
        "Gold surrogate key for legislative voting dimension.",

    "vot_id_votacao":
        "Business identifier of the legislative voting event from the source system.",

    "vot_tx_uri":
        "Voting URI from the source system.",

    "vot_dt_votacao":
        "Voting date.",

    "vot_dh_votacao":
        "Voting timestamp.",

    "vot_nr_ano":
        "Voting year.",

    "vot_nr_mes":
        "Voting month number.",

    "vot_tx_descricao":
        "Voting description.",

    "vot_tx_resultado":
        "Voting result.",

    "vot_tx_aprovacao":
        "Voting approval status.",

    "vot_tx_sigla_orgao":
        "Legislative body acronym associated with the voting event.",

    "vot_tx_nome_orgao":
        "Legislative body name associated with the voting event.",

    "prop_id_proposicao":
        "Business identifier of the related proposition, when available.",

    "prop_tx_sigla_tipo":
        "Type acronym of the related proposition, when available.",

    "prop_nr_numero":
        "Proposition number, when available.",

    "prop_nr_ano":
        "Proposition year, when available.",

    "vot_fl_registro_valido_silver":
        "Flag indicating whether record passed Silver validation.",

    "vot_fl_registro_valido_gold":
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
    message="Gold legislative voting dimension generated successfully.",
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
print("DIMENSÃO VOTAÇÕES - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read: {records_read}")
print(f"Records written: {records_written}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(gold_df.limit(20))
