# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC # 08 Gold — Parliamentary Inquiry Committees Dimension
# MAGIC
# MAGIC **Notebook:** `08_dm_cpis`
# MAGIC
# MAGIC Builds the curated Gold parliamentary inquiry committees dimension used by analytical models and business marts.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Parliamentary inquiry committee dimensional model
# MAGIC * Parliamentary inquiry committee surrogate key generation
# MAGIC * Parliamentary inquiry committee business identifiers
# MAGIC * Parliamentary inquiry committee descriptive attributes
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
# MAGIC * Read validated parliamentary inquiry committee records from Silver
# MAGIC * Keep one analytical record per parliamentary inquiry committee
# MAGIC * Create the parliamentary inquiry committee surrogate key
# MAGIC * Preserve business identifiers and descriptive attributes
# MAGIC * Preserve audit and traceability information
# MAGIC * Generate Gold execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Gold quality validations
# MAGIC * Publish the Gold parliamentary inquiry committees dimension
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dimensional Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per parliamentary inquiry committee.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC `brazil_legislative_analytics.silver.slv_cpis`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.gold.dm_cpis`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `cpi_id_orgao`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC `cpi_sk_cpi`
# MAGIC
# MAGIC ### Main Analytical Attributes
# MAGIC
# MAGIC * Parliamentary inquiry committee acronym
# MAGIC * Parliamentary inquiry committee name
# MAGIC * Parliamentary inquiry committee nickname
# MAGIC * Committee type and type description
# MAGIC * Committee scope
# MAGIC * Committee analytical status
# MAGIC * Start and end dates
# MAGIC * Legislature
# MAGIC * Active flag
# MAGIC * Mixed committee flag
# MAGIC * Governance attributes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC ### Rule 1 — Silver Valid Records
# MAGIC
# MAGIC Only records approved during Silver validation are eligible for Gold.
# MAGIC
# MAGIC ```python
# MAGIC cpi_fl_registro_valido_silver = true
# MAGIC ```
# MAGIC
# MAGIC ### Rule 2 — One Record Per Parliamentary Inquiry Committee
# MAGIC
# MAGIC Only one analytical record is maintained for each parliamentary inquiry committee.
# MAGIC
# MAGIC ```python
# MAGIC cpi_id_orgao
# MAGIC ```
# MAGIC
# MAGIC must be unique in the Gold dimension.
# MAGIC
# MAGIC ### Rule 3 — Gold Surrogate Key Generation
# MAGIC
# MAGIC A deterministic surrogate key is generated using the business key.
# MAGIC
# MAGIC ```python
# MAGIC cpi_sk_cpi = sha2(cpi_id_orgao)
# MAGIC ```
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
# MAGIC * Null parliamentary inquiry committee acronyms
# MAGIC * Null parliamentary inquiry committee names
# MAGIC * Duplicate parliamentary inquiry committee identifiers
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
# MAGIC * Parliamentary inquiry committee records are derived from validated Silver data.
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

NOTEBOOK_NAME = "08_dm_cpis"

ENTITY_NAME = "cpis"

SOURCE_TABLE = f"{SILVER_SCHEMA}.slv_cpis"

TARGET_TABLE = f"{GOLD_SCHEMA}.dm_cpis"

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
        F.col("cpi_fl_registro_valido_silver") == True
    )
)

# COMMAND ----------

# ============================================================
# GOLD SURROGATE KEY
# ============================================================

df_gold = (
    df_gold
    .withColumn(
        "cpi_sk_cpi",
        F.sha2(
            F.col("cpi_id_orgao").cast("string"),
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
        "cpi_fl_registro_valido_gold",
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
                F.col("cpi_id_orgao").cast("string"),
                F.col("cpi_tx_sigla").cast("string"),
                F.col("cpi_tx_nome").cast("string"),
                F.col("cpi_tx_status_analitico").cast("string"),
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
        "cpi_id_orgao",
        "cpi_tx_sigla",
        "cpi_tx_nome",
        "cpi_sk_cpi"
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_gold,
    key_columns=[
        "cpi_id_orgao"
    ]
)

null_results = validate_nulls(
    dataframe=df_gold,
    columns=[
        "cpi_id_orgao",
        "cpi_tx_sigla",
        "cpi_tx_nome",
        "cpi_sk_cpi"
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
Gold parliamentary inquiry committees dimension.

This dimension contains one record per Brazilian parliamentary inquiry committee.

Main characteristics:

* surrogate key
* business key
* analytical attributes
* Silver lineage
* Gold lineage
* governance metadata
"""

COLUMN_COMMENTS = {
    "cpi_sk_cpi":
        "Gold surrogate key for parliamentary inquiry committee dimension.",

    "cpi_id_orgao":
        "Business identifier of the parliamentary inquiry committee body from the source system.",

    "cpi_tx_sigla":
        "Standardized parliamentary inquiry committee acronym.",

    "cpi_tx_nome":
        "Parliamentary inquiry committee name.",

    "cpi_tx_apelido":
        "Parliamentary inquiry committee nickname or short name.",

    "cpi_tx_tipo":
        "Parliamentary inquiry committee type code or acronym.",

    "cpi_tx_tipo_descricao":
        "Parliamentary inquiry committee type description.",

    "cpi_tx_tipo_orgao":
        "Source body type associated with the parliamentary inquiry committee.",

    "cpi_tx_abrangencia":
        "Parliamentary inquiry committee scope.",

    "cpi_tx_situacao_origem":
        "Original status description from the source system.",

    "cpi_tx_status_analitico":
        "Curated analytical status assigned during Silver processing.",

    "cpi_dt_inicio":
        "Parliamentary inquiry committee start date.",

    "cpi_dt_fim":
        "Parliamentary inquiry committee end date.",

    "cpi_nr_ano_inicio":
        "Year when the parliamentary inquiry committee started.",

    "leg_id_legislatura":
        "Legislature identifier associated with the parliamentary inquiry committee.",

    "cpi_tx_uri":
        "Parliamentary inquiry committee URI from the source system.",

    "cpi_fl_mista":
        "Flag indicating whether the inquiry committee is a mixed committee.",

    "cpi_fl_ativa":
        "Flag indicating whether the parliamentary inquiry committee is active.",

    "cpi_fl_registro_valido_silver":
        "Flag indicating whether record passed Silver validation.",

    "cpi_fl_registro_valido_gold":
        "Flag indicating whether record passed Gold validation.",

    "aud_id_execucao_gold":
        "Execution identifier generated during Gold processing.",

    "aud_dh_processamento_gold":
        "Timestamp when the record was processed in Gold.",

    "aud_tx_versao_pipeline_gold":
        "Pipeline version used during Gold processing.",

    "aud_tx_hash_registro_gold":
        "Deterministic Gold record hash.",

    "aud_tx_camada_origem":
    "Source data layer used during Gold CPI dimension processing.",

    "aud_tx_tabela_origem":
        "Source Silver table used to derive CPI dimension records.",

    "aud_tx_tabela_destino":
        "Destination Gold table where CPI dimension records are persisted.",

    "aud_tx_versao_pipeline_silver":
        "Silver pipeline version that produced the source CPI records."   
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
    message="Gold parliamentary inquiry committees dimension generated successfully.",
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
print("DIMENSÃO CPIS - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read: {records_read}")
print(f"Records written: {records_written}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(gold_df.limit(20))
