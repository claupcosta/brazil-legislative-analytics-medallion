# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Silver — Rejected Records Table
# MAGIC
# MAGIC **Notebook:** `00_silver_registros_rejeitados`
# MAGIC
# MAGIC Creates the standardized Silver rejected records table used to store
# MAGIC records that fail Silver validation rules and should not be silently discarded.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Rejected records schema
# MAGIC - Silver rejection metadata
# MAGIC - Source lineage fields
# MAGIC - Validation rule tracking
# MAGIC - Original payload preservation
# MAGIC - Governance comments
# MAGIC - Delta table creation logic
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Create the generic Silver rejected records table
# MAGIC - Standardize rejection tracking fields
# MAGIC - Preserve original source payloads
# MAGIC - Preserve source table lineage
# MAGIC - Register validation rule and rejection reason
# MAGIC - Support auditability and traceability
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Rejected records are not deleted silently
# MAGIC - This table supports all Silver entities
# MAGIC - Business aggregations remain isolated in Gold
# MAGIC - Naming conventions follow Portuguese mnemonic standards
# MAGIC - Comments and documentation are written in English
# MAGIC - This table improves data quality explainability across the pipeline
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/decisions/silver_layer_strategy.md`
# MAGIC - `/docs/governance/data_quality.md`
# MAGIC - `/docs/governance/traceability.md`
# MAGIC - `/docs/operations/execution_guide.md`
# MAGIC - `/docs/standards/naming_conventions.md`

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

from datetime import datetime
import uuid

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("00 - SILVER REGISTROS REJEITADOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "00_silver_registros_rejeitados"
LAYER_NAME = "silver"
ENTITY_NAME = "registros_rejeitados"

TARGET_TABLE = get_silver_table(
    SILVER_TABLES["registros_rejeitados"]
)

execution_id = str(uuid.uuid4())
started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

# COMMAND ----------

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def apply_table_and_column_comments(
    target_table: str,
    table_comment: str,
    column_comments: dict,
) -> None:
    """
    Applies table and column comments for Unity Catalog governance documentation.
    """

    escaped_table_comment = table_comment.replace("'", "\\'")

    spark.sql(f"""
    COMMENT ON TABLE {target_table}
    IS '{escaped_table_comment}'
    """)

    for column_name, column_comment in column_comments.items():

        escaped_column_comment = column_comment.replace("'", "\\'")

        spark.sql(f"""
        ALTER TABLE {target_table}
        ALTER COLUMN {column_name}
        COMMENT '{escaped_column_comment}'
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start Pipeline Log

# COMMAND ----------

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver rejected records table creation started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver rejected records table creation.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Rejected Records Table

# COMMAND ----------

try:

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
    (
        rej_id_registro STRING,
        aud_id_execucao_silver STRING,
        aud_dh_processamento TIMESTAMP,
        aud_tx_camada_origem STRING,
        aud_tx_tabela_origem STRING,
        aud_tx_tabela_destino STRING,
        aud_tx_versao_pipeline_silver STRING,
        rej_tx_entidade STRING,
        rej_tx_id_registro STRING,
        rej_tx_regra_validacao STRING,
        rej_tx_motivo_rejeicao STRING,
        rej_tx_severidade STRING,
        rej_tx_payload_json STRING,
        rej_tx_observacao STRING
    )
    USING DELTA
    """)

    log_info(
        pipeline_logger=logger,
        message=(
            f"Silver rejected records table created or already exists "
            f"| target_table={TARGET_TABLE}"
        ),
    )

except Exception as error:

    finished_at = datetime.now()

    duration_seconds = (
        finished_at - started_at
    ).total_seconds()

    write_pipeline_log(
        log_id=str(uuid.uuid4()),
        execution_id=execution_id,
        notebook_name=NOTEBOOK_NAME,
        layer_name=LAYER_NAME,
        entity_name=ENTITY_NAME,
        target_table=TARGET_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=(
            f"Failed creating Silver rejected records table "
            f"| error={str(error)}"
        ),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=0,
        records_written=0,
    )

    log_error(
        pipeline_logger=logger,
        message="Failed creating Silver rejected records table.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply Governance Comments

# COMMAND ----------

table_comment = """
Generic Silver rejected records table.

This table stores records rejected during Silver validation processes.
Rejected records are preserved with validation rule, rejection reason,
source lineage, processing metadata and original payload.

Main characteristics:
- generic rejection tracking across Silver entities
- original payload preservation
- validation rule traceability
- rejection reason documentation
- source and target table lineage
- auditability and replay support

Silver layer note:
- Rejected records must not be silently discarded.
- This table improves data quality explainability and governance transparency.
"""

column_comments = {
    "rej_id_registro": "Unique rejected record identifier generated during Silver processing.",
    "aud_id_execucao_silver": "Silver execution identifier responsible for the rejection.",
    "aud_dh_processamento": "Timestamp when the rejected record was processed in the Silver layer.",
    "aud_tx_camada_origem": "Source Medallion layer of the rejected record.",
    "aud_tx_tabela_origem": "Fully qualified source table of the rejected record.",
    "aud_tx_tabela_destino": "Fully qualified intended target Silver table.",
    "aud_tx_versao_pipeline_silver": "Pipeline version responsible for the Silver validation.",
    "rej_tx_entidade": "Entity name associated with the rejected record.",
    "rej_tx_id_registro": "Original business or technical identifier of the rejected record.",
    "rej_tx_regra_validacao": "Validation rule that rejected the record.",
    "rej_tx_motivo_rejeicao": "Detailed rejection reason.",
    "rej_tx_severidade": "Severity level of the rejection.",
    "rej_tx_payload_json": "Original payload preserved for auditability and replay.",
    "rej_tx_observacao": "Additional technical observation about the rejection.",
}

try:

    apply_table_and_column_comments(
        target_table=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

    log_info(
        pipeline_logger=logger,
        message="Governance comments applied to rejected records table.",
    )

except Exception as error:

    log_error(
        pipeline_logger=logger,
        message="Failed applying governance comments to rejected records table.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Display Table Structure

# COMMAND ----------

display(
    spark.sql(
        f"DESCRIBE TABLE {TARGET_TABLE}"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Final Pipeline Log

# COMMAND ----------

finished_at = datetime.now()

duration_seconds = (
    finished_at - started_at
).total_seconds()

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_SUCCESS,
    message="Silver rejected records table created successfully.",
    started_at=started_at,
    finished_at=finished_at,
    duration_seconds=duration_seconds,
    records_read=0,
    records_written=0,
)

log_success(
    pipeline_logger=logger,
    message=(
        "Silver rejected records table created successfully "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("SILVER REGISTROS REJEITADOS COMPLETED")
print("=" * 90)
print(f"Target Table: {TARGET_TABLE}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)