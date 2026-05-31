# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Bodies CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `07a_bronze_orgaos_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `Official CSV fallback files`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_orgaos`
# MAGIC
# MAGIC Loads legislative body records from official CSV fallback files
# MAGIC stored in Unity Catalog Volume and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw source fidelity,
# MAGIC including ingestion metadata, original file lineage and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Discover legislative body CSV files dynamically
# MAGIC - Load official Câmara CSV fallback files
# MAGIC - Standardize Bronze ingestion columns
# MAGIC - Preserve raw source payloads
# MAGIC - Generate ingestion traceability metadata
# MAGIC - Generate deterministic record hashes
# MAGIC - Persist Bronze Delta tables
# MAGIC - Register operational execution logs
# MAGIC - Preserve original source file lineage
# MAGIC - Apply governance comments to tables and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves source-system extraction fidelity
# MAGIC - CSV fallback is the recommended operational ingestion strategy
# MAGIC - The `/orgaos` API endpoint may present timeout, instability and intermittent availability behavior
# MAGIC - CSV fallback ingestion was implemented to improve operational resilience and execution stability
# MAGIC - Original file lineage is preserved for auditability
# MAGIC - CPI classification is intentionally handled in Silver and Gold layers
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - Governance comments are applied to tables and columns
# MAGIC - This notebook prioritizes stable historical ingestion from official source files
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/decisions/api_limitations.md`
# MAGIC - `/docs/governance/data_lineage.md`
# MAGIC - `/docs/governance/data_quality_rules.md`

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_comments

# COMMAND ----------


from datetime import datetime
import uuid

from pyspark.sql.functions import (
    lit,
    current_timestamp,
    col,
    to_json,
    struct,
)

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("07A - BRONZE ORGAOS CSV FALLBACK")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "07a_bronze_orgaos_csv_fallback"

LAYER_NAME = "bronze"

ENTITY_NAME = "orgaos"

SOURCE_FILE_PATH = VOLUME_RAW_ORGAOS

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["orgaos"]
)

LOAD_TYPE = LOAD_TYPE_FALLBACK

execution_id = str(uuid.uuid4())

started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

# COMMAND ----------

# ============================================================
# INGESTION CONFIGURATION
# ============================================================

CSV_SEPARATOR = ";"

CSV_ENCODING = "UTF-8"

OUTPUT_REPARTITION = 4

APPLY_GOVERNANCE_COMMENTS = True

# COMMAND ----------

def apply_table_and_column_comments(
    target_table: str,
    table_comment: str,
    column_comments: dict,
) -> None:
    """
    Applies governance comments to table and columns.
    """

    escaped_table_comment = table_comment.replace("'", "\\'")

    spark.sql(f"""
    COMMENT ON TABLE {target_table}
    IS '{escaped_table_comment}'
    """)

    for column_name, column_comment in column_comments.items():

        escaped_column_comment = (
            column_comment.replace("'", "\\'")
        )

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
    message="Bronze orgaos CSV fallback ingestion started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting orgaos CSV fallback ingestion.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Discover CSV Files Dynamically

# COMMAND ----------

try:

    source_files = dbutils.fs.ls(
        SOURCE_FILE_PATH
    )

    csv_files = []

    ignored_files = []

    for file_info in source_files:

        file_path = file_info.path

        if file_path.lower().endswith(".csv"):

            csv_files.append(file_path)

        else:

            ignored_files.append(file_path)

    csv_files = sorted(csv_files)

    if len(csv_files) == 0:

        raise Exception(
            f"No valid orgaos CSV files found "
            f"| source_path={SOURCE_FILE_PATH}"
        )

    log_info(
        pipeline_logger=logger,
        message=(
            f"Orgaos CSV files discovered dynamically "
            f"| files={len(csv_files)}"
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
            f"Failed discovering orgaos CSV files "
            f"| error={str(error)}"
        ),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=None,
        records_written=None,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read CSV Files

# COMMAND ----------

try:

    raw_csv_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", CSV_SEPARATOR)
        .option("encoding", CSV_ENCODING)
        .option("multiLine", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("mode", "PERMISSIVE")
        .csv(csv_files)
        .withColumn(
            "aud_tx_arquivo_origem",
            col("_metadata.file_path")
            .cast("string")
        )
    )

    records_read = raw_csv_df.count()

    log_info(
        pipeline_logger=logger,
        message=(
            f"Orgaos CSV records loaded successfully "
            f"| records_read={records_read}"
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
            f"Failed reading orgaos CSV files "
            f"| error={str(error)}"
        ),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=None,
        records_written=None,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Raw Payload

# COMMAND ----------

source_columns = raw_csv_df.columns

bronze_df = (
    raw_csv_df
    .withColumn(
        "org_tx_payload_json",
        to_json(
            struct(
                *[
                    col(column_name)
                    for column_name in source_columns
                ]
            )
        ).cast("string")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Standardize Bronze Columns

# COMMAND ----------

from pyspark.sql.functions import regexp_extract

bronze_df = (
    bronze_df

    # ============================================================
    # ORGANIZATION IDENTIFIER
    # ============================================================

    .withColumn(
        "org_id_orgao",
        regexp_extract(
            col("uri"),
            r"/orgaos/([0-9]+)",
            1
        )
    )

    # ============================================================
    # BUSINESS COLUMNS
    # ============================================================

    .withColumn(
        "org_tx_sigla",
        col("sigla").cast("string")
    )

    .withColumn(
        "org_tx_apelido",
        col("apelido").cast("string")
    )

    .withColumn(
        "org_tx_nome",
        col("nome").cast("string")
    )

    .withColumn(
        "org_tx_tipo_orgao",
        col("tipoOrgao").cast("string")
    )

    .withColumn(
        "org_tx_sigla_tipo_orgao",
        col("codTipoOrgao").cast("string")
    )

    .withColumn(
        "org_tx_situacao",
        col("descricaoSituacao").cast("string")
    )

    .withColumn(
        "org_dt_inicio",
        col("dataInicio").cast("string")
    )

    .withColumn(
        "org_dt_fim",
        col("dataFim").cast("string")
    )

    .withColumn(
        "org_tx_uri",
        col("uri").cast("string")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ensure Mandatory Columns and Explicit Types

# COMMAND ----------

expected_columns = [

    "org_id_orgao",
    "org_tx_sigla",
    "org_tx_nome",
    "org_tx_apelido",
    "org_tx_tipo_orgao",
    "org_tx_sigla_tipo_orgao",
    "org_tx_situacao",
    "org_dt_inicio",
    "org_dt_fim",
    "org_tx_uri",
    "org_tx_payload_json",
    "aud_id_execucao",
    "aud_dh_ingestao",
    "aud_tx_endpoint_origem",
    "aud_tx_sistema_origem",
    "aud_tx_versao_pipeline",
    "aud_tx_tipo_carga",
    "aud_tx_arquivo_origem",
]

for expected_column in expected_columns:

    if expected_column not in bronze_df.columns:

        bronze_df = bronze_df.withColumn(
            expected_column,
            lit(None).cast("string")
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Explicit Casts to Avoid VOID Columns

# COMMAND ----------

bronze_df = (
    bronze_df

    .withColumn(
        "org_id_orgao",
        col("org_id_orgao").cast("string")
    )

    .withColumn(
        "org_tx_sigla",
        col("org_tx_sigla").cast("string")
    )

    .withColumn(
        "org_tx_nome",
        col("org_tx_nome").cast("string")
    )

    .withColumn(
        "org_tx_apelido",
        col("org_tx_apelido").cast("string")
    )

    .withColumn(
        "org_tx_tipo_orgao",
        col("org_tx_tipo_orgao").cast("string")
    )

    .withColumn(
        "org_tx_sigla_tipo_orgao",
        col("org_tx_sigla_tipo_orgao").cast("string")
    )

    .withColumn(
        "org_tx_situacao",
        col("org_tx_situacao").cast("string")
    )

    .withColumn(
        "org_dt_inicio",
        col("org_dt_inicio").cast("string")
    )

    .withColumn(
        "org_dt_fim",
        col("org_dt_fim").cast("string")
    )

    .withColumn(
        "org_tx_uri",
        col("org_tx_uri").cast("string")
    )

    .withColumn(
        "org_tx_payload_json",
        col("org_tx_payload_json").cast("string")
    )

    .withColumn(
        "aud_id_execucao",
        lit(execution_id).cast("string")
    )

    .withColumn(
        "aud_dh_ingestao",
        current_timestamp()
    )

    .withColumn(
        "aud_tx_endpoint_origem",
        lit(SOURCE_FILE_PATH).cast("string")
    )

    .withColumn(
        "aud_tx_sistema_origem",
        lit("csv_fallback").cast("string")
    )

    .withColumn(
        "aud_tx_versao_pipeline",
        lit(PROJECT_VERSION).cast("string")
    )

    .withColumn(
        "aud_tx_tipo_carga",
        lit(LOAD_TYPE).cast("string")
    )

    .withColumn(
        "aud_tx_arquivo_origem",
        col("aud_tx_arquivo_origem").cast("string")
    )
)

# COMMAND ----------

bronze_df = bronze_df.select(
    *expected_columns
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Add Record Hash

# COMMAND ----------

bronze_df = add_hash(
    dataframe=bronze_df,
    columns=[
        "org_id_orgao",
        "org_tx_payload_json",
    ],
    hash_column="aud_tx_hash_registro",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Remove Technical Duplicates

# COMMAND ----------

bronze_df = (
    bronze_df
    .dropDuplicates(
        ["aud_tx_hash_registro"]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Persist Bronze Table

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS {TARGET_TABLE}
""")

# COMMAND ----------

bronze_df = bronze_df.repartition(
    OUTPUT_REPARTITION
)

# COMMAND ----------

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

records_written = bronze_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze orgaos CSV fallback table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Apply Governance Comments

# COMMAND ----------

table_comment = """
Raw legislative bodies ingestion table from official CSV fallback files.

This Bronze table preserves legislative body records loaded from CSV files
stored in Unity Catalog Volume.

Main characteristics:
- CSV fallback ingestion
- legislative body metadata
- original file lineage
- original payload preservation
- ingestion metadata
- deterministic hash support
- auditability

Fallback decision:
- The Câmara API endpoint /orgaos may present timeout and instability behavior.
- CSV fallback was implemented to preserve operational continuity.

Architecture decision:
- CPI classification is intentionally handled in Silver/Gold layers.

Bronze layer note:
- Technical duplicates and business validation are intentionally handled in Silver.
"""

column_comments = {

    "org_id_orgao":
        "Legislative body identifier as provided by the CSV source.",

    "org_tx_sigla":
        "Legislative body acronym as provided by the CSV source.",

    "org_tx_nome":
        "Legislative body name as provided by the CSV source.",

    "org_tx_apelido":
        "Legislative body nickname as provided by the CSV source.",

    "org_tx_tipo_orgao":
        "Legislative body type description as provided by the CSV source.",

    "org_tx_sigla_tipo_orgao":
        "Legislative body type acronym as provided by the CSV source.",

    "org_tx_situacao":
        "Legislative body status as provided by the CSV source.",

    "org_dt_inicio":
        "Legislative body start date as provided by the CSV source.",

    "org_dt_fim":
        "Legislative body end date as provided by the CSV source.",

    "org_tx_uri":
        "Legislative body URI as provided by the CSV source.",

    "org_tx_payload_json":
        "Original raw JSON payload generated from the CSV record.",

    "aud_id_execucao":
        "Unique execution identifier for the ingestion run.",

    "aud_dh_ingestao":
        "Timestamp when the record was ingested into Bronze layer.",

    "aud_tx_endpoint_origem":
        "Source volume path used during ingestion.",

    "aud_tx_sistema_origem":
        "Source system identifier.",

    "aud_tx_versao_pipeline":
        "Pipeline version identifier.",

    "aud_tx_tipo_carga":
        "Load type applied during ingestion.",

    "aud_tx_arquivo_origem":
        "Original CSV source file path.",

    "aud_tx_hash_registro":
        "Deterministic hash used for lineage and deduplication.",
}

# COMMAND ----------

if APPLY_GOVERNANCE_COMMENTS:

    apply_table_and_column_comments(
        target_table=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Final Pipeline Log

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
    message=(
        f"Bronze orgaos CSV fallback ingestion completed successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written}"
    ),
    started_at=started_at,
    finished_at=finished_at,
    duration_seconds=duration_seconds,
    records_read=records_read,
    records_written=records_written,
)

log_success(
    pipeline_logger=logger,
    message=(
        f"Bronze orgaos CSV fallback ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE ORGAOS CSV FALLBACK COMPLETED")
print("=" * 90)
print(f"Source Path: {SOURCE_FILE_PATH}")
print(f"CSV Files: {len(csv_files)}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)