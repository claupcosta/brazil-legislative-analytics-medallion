# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC
# MAGIC # 02 Gold — Political Parties Dimension
# MAGIC
# MAGIC **Notebook:** `02_dm_partidos`
# MAGIC
# MAGIC Builds the curated Gold political party dimension used by analytical models and business marts.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Political party dimensional model
# MAGIC * Political party surrogate key generation
# MAGIC * Political party business identifiers
# MAGIC * Political party descriptive attributes
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
# MAGIC * Read validated political party records from Silver
# MAGIC * Keep one analytical record per political party
# MAGIC * Create the political party surrogate key
# MAGIC * Preserve business identifiers and descriptive attributes
# MAGIC * Preserve audit and traceability information
# MAGIC * Generate Gold execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Gold quality validations
# MAGIC * Publish the Gold political party dimension
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dimensional Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per political party.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC `brazil_legislative_analytics.silver.slv_partidos`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.gold.dm_partidos`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `par_id_partido`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC `par_sk_partido`
# MAGIC
# MAGIC ### Main Analytical Attributes
# MAGIC
# MAGIC * Political Party Acronym
# MAGIC * Political Party Name
# MAGIC * Political Party URI
# MAGIC * Source Registration Type
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
# MAGIC ```python
# MAGIC par_fl_registro_valido_silver = true
# MAGIC ```
# MAGIC
# MAGIC ### Rule 2 — One Record Per Political Party
# MAGIC
# MAGIC Only one analytical record is maintained for each political party.
# MAGIC
# MAGIC ```python
# MAGIC par_id_partido
# MAGIC ```
# MAGIC
# MAGIC must be unique in the Gold dimension.
# MAGIC
# MAGIC ### Rule 3 — Gold Surrogate Key Generation
# MAGIC
# MAGIC A deterministic surrogate key is generated using the business key.
# MAGIC
# MAGIC ```python
# MAGIC par_sk_partido = sha2(par_id_partido)
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
# MAGIC * Null political party acronyms
# MAGIC * Duplicate political party identifiers
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
# MAGIC * Political party records are derived from validated Silver data.
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

NOTEBOOK_NAME = "02_dm_partidos"

ENTITY_NAME = "partidos"

SOURCE_TABLE = f"{SILVER_SCHEMA}.slv_partidos"

TARGET_TABLE = f"{GOLD_SCHEMA}.dm_partidos"

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
    F.col("par_fl_registro_valido_silver") == True
)


)

# COMMAND ----------

# ============================================================

# GOLD SURROGATE KEY

# ============================================================

df_gold = (


df_gold

.withColumn(
    "par_sk_partido",
    F.sha2(
        F.col("par_id_partido"),
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
    "par_fl_registro_valido_gold",
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
            F.col("par_id_partido"),
            F.col("par_tx_sigla")
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
"par_id_partido",
"par_tx_sigla",
"par_sk_partido"
]
)

duplicate_result = validate_duplicates(
dataframe=df_gold,
key_columns=[
"par_id_partido"
]
)

null_results = validate_nulls(
dataframe=df_gold,
columns=[
"par_id_partido",
"par_tx_sigla",
"par_sk_partido"
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
Gold political party dimension.

This dimension contains one record per Brazilian political party.

Main characteristics:

* surrogate key
* business key
* analytical attributes
* Silver lineage
* Gold lineage
* governance metadata
  """

COLUMN_COMMENTS = {


"par_sk_partido":
    "Gold surrogate key for political party dimension.",

"par_id_partido":
    "Deterministic political party identifier generated from the party acronym.",

"par_tx_sigla":
    "Standardized political party acronym.",

"par_tx_nome":
    "Political party name.",

"par_tx_uri":
    "Political party URI.",

"par_fl_registro_valido_gold":
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
message="Gold political party dimension generated successfully.",
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
print("DIMENSÃO PARTIDOS - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read: {records_read}")
print(f"Records written: {records_written}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(gold_df.limit(20))



