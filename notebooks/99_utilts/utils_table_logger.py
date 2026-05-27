# Databricks notebook source
# MAGIC %md
# MAGIC # Utils Layer — Pipeline Table Logger
# MAGIC
# MAGIC **Notebook:** `utils_table_logger`  
# MAGIC **Layer:** `Utils`  
# MAGIC **Source/Endpoint:** `Pipeline Execution Events`  
# MAGIC **Target:** `Audit pipeline execution Delta table`
# MAGIC
# MAGIC Provides reusable functions to persist structured pipeline execution
# MAGIC events into the audit Delta table.
# MAGIC
# MAGIC This notebook centralizes operational execution logging used across
# MAGIC Bronze, Silver, Gold, Marts, Quality and Jobs workflows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Persist structured pipeline execution logs
# MAGIC - Standardize audit log schema generation
# MAGIC - Register execution metadata and processing metrics
# MAGIC - Support operational monitoring and traceability
# MAGIC - Append execution events into audit Delta tables
# MAGIC - Support reusable audit logging workflows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Shared utility notebook across Medallion layers
# MAGIC - Audit tables must be created previously by `00_setup/02_audit_tables`
# MAGIC - This notebook only appends records into existing audit tables
# MAGIC - Supports operational monitoring and troubleshooting workflows
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/monitoring/observability.md`
# MAGIC - `/docs/governance/data_lineage.md`
# MAGIC - `/docs/architecture/medallion_architecture.md`

# COMMAND ----------

# MAGIC %run ./utils_config

# COMMAND ----------

from datetime import datetime
from typing import Optional
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)

# COMMAND ----------

PROJECT_NAME_VALUE = globals().get("PROJECT_NAME", "brazil_legislative_analytics")
PROJECT_VERSION_VALUE = globals().get("PROJECT_VERSION", globals().get("PIPELINE_VERSION", "v1.0.0"))
PROJECT_ENVIRONMENT_VALUE = globals().get("PROJECT_ENVIRONMENT", "dev")

CATALOG_NAME_VALUE = globals().get("CATALOG_NAME", "brazil_legislative_analytics")
SCHEMA_AUDIT_VALUE = globals().get("SCHEMA_AUDIT", "audit")
AUDIT_LOG_TABLE_VALUE = globals().get("AUD_TB_LOG_EXECUCAO_PIPELINE", "aud_log_execucao_pipeline")

PIPELINE_LOG_TABLE = (
    f"{CATALOG_NAME_VALUE}.{SCHEMA_AUDIT_VALUE}.{AUDIT_LOG_TABLE_VALUE}"
)

# COMMAND ----------

def get_pipeline_log_schema() -> StructType:
    """
    Returns the schema used to append records into the pipeline execution audit table.
    """

    return StructType([
        StructField("aud_id_log", StringType(), True),
        StructField("aud_id_execucao", StringType(), True),
        StructField("aud_tx_nome_projeto", StringType(), True),
        StructField("aud_tx_versao_pipeline", StringType(), True),
        StructField("aud_tx_ambiente", StringType(), True),
        StructField("aud_tx_nome_notebook", StringType(), True),
        StructField("aud_tx_nome_camada", StringType(), True),
        StructField("aud_tx_nome_entidade", StringType(), True),
        StructField("aud_tx_tabela_destino", StringType(), True),
        StructField("aud_tx_status", StringType(), True),
        StructField("aud_dh_inicio", TimestampType(), True),
        StructField("aud_dh_fim", TimestampType(), True),
        StructField("aud_nr_duracao_segundos", DoubleType(), True),
        StructField("aud_qt_registros_lidos", LongType(), True),
        StructField("aud_qt_registros_gravados", LongType(), True),
        StructField("aud_tx_mensagem", StringType(), True),
    ])

# COMMAND ----------

def write_pipeline_log(
    log_id: str,
    execution_id: str,
    notebook_name: str,
    layer_name: str,
    entity_name: str,
    target_table: str,
    status: str,
    message: str,
    started_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    duration_seconds: Optional[float] = None,
    records_read: Optional[int] = None,
    records_written: Optional[int] = None,
    project_name: str = PROJECT_NAME_VALUE,
    project_version: str = PROJECT_VERSION_VALUE,
    environment: str = PROJECT_ENVIRONMENT_VALUE,
) -> None:
    """
    Appends a structured pipeline execution log record into the audit table.
    """

    log_schema = get_pipeline_log_schema()

    log_df = spark.createDataFrame(
        [(
            log_id,
            execution_id,
            project_name,
            project_version,
            environment,
            notebook_name,
            layer_name,
            entity_name,
            target_table,
            status,
            started_at,
            finished_at,
            float(duration_seconds) if duration_seconds is not None else None,
            int(records_read) if records_read is not None else None,
            int(records_written) if records_written is not None else None,
            message,
        )],
        log_schema,
    )

    log_df.write.mode("append").saveAsTable(PIPELINE_LOG_TABLE)

# COMMAND ----------

print("utils_table_logger loaded successfully.")