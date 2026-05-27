# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Layer — Audit Tables Initialization
# MAGIC
# MAGIC **Notebook:** `02_audit_tables`  
# MAGIC **Layer:** `Setup`  
# MAGIC **Source/Endpoint:** `Internal Spark SQL Commands`  
# MAGIC **Target:** `Audit and governance Delta tables`
# MAGIC
# MAGIC Creates audit and governance tables used to monitor pipeline executions,
# MAGIC pipeline errors and data quality validations across all Medallion layers.
# MAGIC
# MAGIC This notebook initializes the audit structure required for observability,
# MAGIC traceability and operational monitoring workflows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Create pipeline execution audit tables
# MAGIC - Create pipeline error logging tables
# MAGIC - Create data quality validation tables
# MAGIC - Apply governance comments to tables and columns
# MAGIC - Validate audit table creation results
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Uses Delta Lake tables for audit persistence
# MAGIC - Supports monitoring and troubleshooting workflows
# MAGIC - Table and column comments support governance standards
# MAGIC - Shared audit structure across all Medallion layers
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/governance/data_governance.md`
# MAGIC - `/docs/monitoring/observability.md`

# COMMAND ----------

# MAGIC %run ./01_project_config

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# ============================================================
# CONFIGURATION
# ============================================================

CATALOG_NAME = globals().get(
    "CATALOG_NAME",
    "brazil_legislative_analytics",
)

SCHEMA_AUDIT = globals().get(
    "SCHEMA_AUDIT",
    "audit",
)

PROJECT_VERSION = globals().get(
    "PROJECT_VERSION",
    "v1.0.0",
)

PIPELINE_LOG_TABLE_NAME = globals().get(
    "AUD_TB_LOG_EXECUCAO_PIPELINE",
    "aud_log_execucao_pipeline",
)

PIPELINE_ERROR_TABLE_NAME = globals().get(
    "AUD_TB_LOG_ERROS_PIPELINE",
    "aud_log_erros_pipeline",
)

DATA_QUALITY_LOG_TABLE_NAME = globals().get(
    "AUD_TB_LOG_QUALIDADE_DADOS",
    "aud_log_qualidade_dados",
)

PIPELINE_LOG_TABLE = (
    f"{CATALOG_NAME}."
    f"{SCHEMA_AUDIT}."
    f"{PIPELINE_LOG_TABLE_NAME}"
)

PIPELINE_ERROR_TABLE = (
    f"{CATALOG_NAME}."
    f"{SCHEMA_AUDIT}."
    f"{PIPELINE_ERROR_TABLE_NAME}"
)

DATA_QUALITY_LOG_TABLE = (
    f"{CATALOG_NAME}."
    f"{SCHEMA_AUDIT}."
    f"{DATA_QUALITY_LOG_TABLE_NAME}"
)

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

# ============================================================
# EXECUTION HEADER
# ============================================================

print("=" * 80)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("02 - CREATE AUDIT TABLES")
print("=" * 80)
print(f"Execution Timestamp: {datetime.now()}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Audit Schema: {SCHEMA_AUDIT}")
print(f"Project Version: {PROJECT_VERSION}")
print("=" * 80)

# COMMAND ----------

# ============================================================
# CREATE PIPELINE EXECUTION LOG TABLE
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PIPELINE_LOG_TABLE} (
    aud_id_log STRING COMMENT 'Unique identifier for the pipeline log record.',
    aud_id_execucao STRING COMMENT 'Unique execution identifier shared across notebooks within the same pipeline run.',
    aud_tx_nome_projeto STRING COMMENT 'Project name associated with the execution.',
    aud_tx_versao_pipeline STRING COMMENT 'Pipeline version executed.',
    aud_tx_ambiente STRING COMMENT 'Execution environment, such as dev, qa or prod.',
    aud_tx_nome_notebook STRING COMMENT 'Notebook responsible for the execution.',
    aud_tx_nome_camada STRING COMMENT 'Medallion layer executed: setup, bronze, silver, gold, marts, quality or jobs.',
    aud_tx_nome_entidade STRING COMMENT 'Business or technical entity processed during execution.',
    aud_tx_tabela_destino STRING COMMENT 'Fully qualified target table processed during execution.',
    aud_tx_status STRING COMMENT 'Execution status: STARTED, SUCCESS, FAILED or WARNING.',
    aud_dh_inicio TIMESTAMP COMMENT 'Execution start timestamp.',
    aud_dh_fim TIMESTAMP COMMENT 'Execution end timestamp.',
    aud_nr_duracao_segundos DOUBLE COMMENT 'Execution duration in seconds.',
    aud_qt_registros_lidos BIGINT COMMENT 'Number of records read during processing.',
    aud_qt_registros_gravados BIGINT COMMENT 'Number of records written during processing.',
    aud_tx_mensagem STRING COMMENT 'Additional execution message or processing note.'
)
USING DELTA
COMMENT 'Audit table responsible for storing pipeline execution history across all Medallion layers.'
""")

# COMMAND ----------

# ============================================================
# CREATE PIPELINE ERROR LOG TABLE
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PIPELINE_ERROR_TABLE} (
    err_id_erro STRING COMMENT 'Unique identifier for the error record.',
    aud_id_execucao STRING COMMENT 'Execution identifier associated with the failed pipeline execution.',
    aud_tx_nome_projeto STRING COMMENT 'Project name associated with the error.',
    aud_tx_versao_pipeline STRING COMMENT 'Pipeline version executed.',
    aud_tx_ambiente STRING COMMENT 'Execution environment where the error occurred.',
    aud_tx_nome_notebook STRING COMMENT 'Notebook where the error occurred.',
    aud_tx_nome_camada STRING COMMENT 'Medallion layer where the error occurred.',
    aud_tx_nome_entidade STRING COMMENT 'Business or technical entity being processed when the error occurred.',
    aud_tx_tabela_destino STRING COMMENT 'Fully qualified target table associated with the error.',
    err_tx_nome_etapa STRING COMMENT 'Pipeline step where the error occurred.',
    err_tx_tipo_erro STRING COMMENT 'Error classification or exception type.',
    err_tx_mensagem STRING COMMENT 'Error message returned during execution.',
    err_tx_stacktrace STRING COMMENT 'Complete stack trace captured for troubleshooting.',
    err_dh_ocorrencia TIMESTAMP COMMENT 'Timestamp when the error occurred.'
)
USING DELTA
COMMENT 'Audit table responsible for storing pipeline execution errors and troubleshooting details.'
""")

# COMMAND ----------

# ============================================================
# CREATE DATA QUALITY LOG TABLE
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DATA_QUALITY_LOG_TABLE} (
    qlt_id_log STRING COMMENT 'Unique identifier for the quality validation log.',
    aud_id_execucao STRING COMMENT 'Execution identifier associated with the quality validation.',
    aud_tx_nome_projeto STRING COMMENT 'Project name associated with the validation.',
    aud_tx_versao_pipeline STRING COMMENT 'Pipeline version executed.',
    aud_tx_ambiente STRING COMMENT 'Execution environment associated with the validation.',
    aud_tx_nome_notebook STRING COMMENT 'Notebook responsible for executing the quality validation.',
    aud_tx_nome_camada STRING COMMENT 'Medallion layer validated.',
    aud_tx_nome_entidade STRING COMMENT 'Business or technical entity validated.',
    aud_tx_tabela_destino STRING COMMENT 'Fully qualified table validated.',
    qlt_tx_nome_regra STRING COMMENT 'Name of the executed data quality rule.',
    qlt_tx_descricao_regra STRING COMMENT 'Description of the executed data quality rule.',
    qlt_tx_status_validacao STRING COMMENT 'Validation result: PASSED, FAILED or WARNING.',
    qlt_qt_total_registros BIGINT COMMENT 'Total number of records evaluated.',
    qlt_qt_registros_invalidos BIGINT COMMENT 'Number of invalid records identified.',
    qlt_pc_registros_invalidos DOUBLE COMMENT 'Percentage of invalid records identified during validation.',
    qlt_dh_validacao TIMESTAMP COMMENT 'Timestamp when the validation was executed.',
    qlt_tx_mensagem STRING COMMENT 'Additional validation message or observation.'
)
USING DELTA
COMMENT 'Audit table responsible for storing data quality validation results across all Medallion layers.'
""")

# COMMAND ----------

# ============================================================
# VALIDATE CREATED AUDIT TABLES
# ============================================================

audit_tables_df = spark.sql(f"""
SHOW TABLES IN {CATALOG_NAME}.{SCHEMA_AUDIT}
""")

display(audit_tables_df)

# COMMAND ----------

# ============================================================
# EXECUTION SUMMARY
# ============================================================

print("=" * 80)
print("AUDIT TABLES CREATED SUCCESSFULLY")
print("=" * 80)
print(f"Table: {PIPELINE_LOG_TABLE}")
print(f"Table: {PIPELINE_ERROR_TABLE}")
print(f"Table: {DATA_QUALITY_LOG_TABLE}")
print("=" * 80)