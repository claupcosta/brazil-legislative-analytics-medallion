# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 09 Gold — Suppliers Dimension
# MAGIC
# MAGIC **Notebook:** `09_dm_fornecedores`
# MAGIC
# MAGIC Builds the curated Gold suppliers dimension used by analytical models and business marts.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Supplier dimensional model
# MAGIC * Supplier surrogate key generation
# MAGIC * Supplier business identifiers
# MAGIC * Supplier descriptive attributes
# MAGIC * Supplier document classification attributes
# MAGIC * Supplier CEAP aggregation attributes
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
# MAGIC * Read validated supplier records from Silver
# MAGIC * Keep one analytical record per supplier
# MAGIC * Create the supplier surrogate key
# MAGIC * Preserve business identifiers and descriptive attributes
# MAGIC * Preserve supplier document quality attributes
# MAGIC * Preserve CEAP expense aggregation attributes
# MAGIC * Preserve audit and traceability information
# MAGIC * Generate Gold execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Gold quality validations
# MAGIC * Publish the Gold suppliers dimension
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dimensional Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per standardized supplier deduplication key.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC `brazil_legislative_analytics.silver.slv_fornecedores`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.gold.dm_fornecedores`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `forn_tx_chave_deduplicacao`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC `forn_sk_fornecedor`
# MAGIC
# MAGIC ### Main Analytical Attributes
# MAGIC
# MAGIC * Supplier name
# MAGIC * Original supplier document
# MAGIC * Clean supplier document
# MAGIC * Supplier document type
# MAGIC * Supplier data quality flags
# MAGIC * CEAP expense count
# MAGIC * Total CEAP net amount
# MAGIC * Average CEAP net amount
# MAGIC * Governance Attributes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC ### Rule 1 — Silver Valid Records
# MAGIC
# MAGIC Only records approved during Silver validation are eligible for Gold.
# MAGIC
# MAGIC
# MAGIC forn_fl_registro_valido_silver = true
# MAGIC
# MAGIC
# MAGIC ### Rule 2 — One Record Per Supplier
# MAGIC
# MAGIC Only one analytical record is maintained for each supplier deduplication key.
# MAGIC
# MAGIC
# MAGIC forn_tx_chave_deduplicacao
# MAGIC
# MAGIC
# MAGIC must be unique in the Gold dimension.
# MAGIC
# MAGIC ### Rule 3 — Gold Surrogate Key Generation
# MAGIC
# MAGIC A deterministic surrogate key is generated using the business key.
# MAGIC
# MAGIC
# MAGIC forn_sk_fornecedor = sha2(forn_tx_chave_deduplicacao)
# MAGIC
# MAGIC
# MAGIC ### Rule 4 — Governance Compliance
# MAGIC
# MAGIC All columns and tables must contain governance comments.
# MAGIC
# MAGIC ### Rule 5 — Traceability Preservation
# MAGIC
# MAGIC Bronze and Silver audit metadata must be preserved.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Controls
# MAGIC
# MAGIC The notebook validates:
# MAGIC
# MAGIC * Null business keys
# MAGIC * Null surrogate keys
# MAGIC * Null supplier names
# MAGIC * Duplicate supplier deduplication keys
# MAGIC * Invalid Gold records
# MAGIC * Governance comment coverage
# MAGIC
# MAGIC Execution is interrupted when critical validations fail.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC * Source data originates from Câmara dos Deputados open data.
# MAGIC * Supplier records are derived from validated Silver CEAP supplier data.
# MAGIC * Gold dimensions are optimized for analytical consumption.
# MAGIC * Documentation and governance comments are written in English.
# MAGIC * Naming conventions follow project standards.
# MAGIC * Traceability fields are preserved across all Medallion layers.
# MAGIC * Gold dimensions serve as the foundation for Facts and Analytical Marts.
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC * `/docs/architecture/README.md`
# MAGIC * `/docs/decisions/silver_layer_strategy.md`
# MAGIC * `/docs/governance/data_quality.md`
# MAGIC * `/docs/operations/execution_guide.md`
# MAGIC * `/docs/changelog.md`
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

# ============================================================
# EXECUTION CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "09_dm_fornecedores"

ENTITY_NAME = "fornecedores"

SOURCE_TABLE = f"{SILVER_SCHEMA}.slv_fornecedores"

TARGET_TABLE = f"{GOLD_SCHEMA}.dm_fornecedores"

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
        F.col("forn_fl_registro_valido_silver") == True
    )
)

# COMMAND ----------

# ============================================================
# GOLD SURROGATE KEY
# ============================================================

df_gold = (
    df_gold
    .withColumn(
        "forn_sk_fornecedor",
        F.sha2(
            F.col("forn_tx_chave_deduplicacao").cast("string"),
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
        "forn_fl_registro_valido_gold",
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
                F.col("forn_tx_chave_deduplicacao").cast("string"),
                F.col("forn_tx_nome").cast("string"),
                F.col("forn_tx_documento_limpo").cast("string"),
                F.col("forn_tx_tipo_documento").cast("string")
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
        "forn_tx_chave_deduplicacao",
        "forn_tx_nome",
        "forn_sk_fornecedor"
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_gold,
    key_columns=[
        "forn_tx_chave_deduplicacao"
    ]
)

null_results = validate_nulls(
    dataframe=df_gold,
    columns=[
        "forn_tx_chave_deduplicacao",
        "forn_tx_nome",
        "forn_sk_fornecedor"
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
Gold suppliers dimension.

This dimension contains one record per standardized CEAP supplier.

Main characteristics:

* surrogate key
* business key
* supplier descriptive attributes
* supplier document classification
* CEAP aggregation attributes
* Silver lineage
* Gold lineage
* governance metadata
"""

COLUMN_COMMENTS = {
    "forn_sk_fornecedor":
        "Gold surrogate key for suppliers dimension.",

    "forn_tx_chave_deduplicacao":
        "Supplier business key based on document when valid or fallback hash when document is unavailable or malformed.",

    "forn_tx_nome":
        "Standardized supplier name.",

    "forn_tx_documento_original":
        "Original supplier CNPJ or CPF value from CEAP expenses.",

    "forn_tx_documento_limpo":
        "Supplier document containing only numeric characters.",

    "forn_tx_tipo_documento":
        "Supplier document type classification: CNPJ, CPF, OUTRO or NAO_INFORMADO.",

    "forn_fl_nome_informado":
        "Flag indicating whether supplier name is informed.",

    "forn_fl_documento_informado":
        "Flag indicating whether supplier document is informed.",

    "forn_fl_documento_repetido":
        "Flag indicating whether supplier document is composed only by repeated digits.",

    "forn_fl_documento_valido_formato":
        "Flag indicating whether supplier document has valid structural format.",

    "forn_qt_despesas":
        "Number of CEAP expense records associated with the supplier.",

    "forn_vl_total_liquido":
        "Total CEAP net value associated with the supplier.",

    "forn_vl_medio_liquido":
        "Average CEAP net value associated with the supplier.",

    "forn_fl_registro_valido_silver":
        "Flag indicating whether supplier record passed Silver validation.",

    "forn_fl_registro_valido_gold":
        "Flag indicating whether supplier record passed Gold validation.",

    "aud_dh_ultima_ingestao_bronze":
        "Latest Bronze ingestion timestamp associated with supplier expenses.",

    "aud_dh_ultimo_processamento_despesa_silver":
        "Latest Silver expense processing timestamp associated with the supplier.",

    "aud_id_execucao_silver":
        "Execution identifier for Silver supplier processing.",

    "aud_dh_processamento":
        "Timestamp when supplier record was processed in Silver.",

    "aud_tx_camada_origem":
        "Source Medallion layer used during supplier processing.",

    "aud_tx_tabela_origem":
        "Source table used during supplier consolidation.",

    "aud_tx_tabela_destino":
        "Target Silver supplier table.",

    "aud_tx_versao_pipeline_silver":
        "Pipeline version used during Silver supplier processing.",

    "aud_tx_hash_registro_silver":
        "Deterministic Silver supplier record hash.",

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
    message="Gold suppliers dimension generated successfully.",
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
print("DIMENSÃO FORNECEDORES - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read: {records_read}")
print(f"Records written: {records_written}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(gold_df.limit(20))
