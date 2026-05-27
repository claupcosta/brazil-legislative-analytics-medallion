# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer — Parliamentary Fronts API Ingestion
# MAGIC
# MAGIC **Notebook:** `02_bronze_frentes`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `/frentes`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_frentes`
# MAGIC
# MAGIC Extracts parliamentary front records from the Câmara dos Deputados
# MAGIC Open Data API and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw API extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Extract parliamentary front records from the Câmara API
# MAGIC - Handle controlled pagination-based ingestion strategy
# MAGIC - Preserve raw API payloads
# MAGIC - Generate ingestion traceability metadata
# MAGIC - Generate deterministic record hashes
# MAGIC - Persist Bronze Delta tables
# MAGIC - Register operational execution logs
# MAGIC - Apply governance comments to tables and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves source-system extraction fidelity
# MAGIC - The `/frentes` endpoint does not support some standard query parameters
# MAGIC - The `/frentes` endpoint may return HTTP 400 when unsupported filters are used
# MAGIC - This notebook intentionally keeps extraction parameters minimal to improve API compatibility
# MAGIC - Legislature association is intentionally handled in downstream layers
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - Governance comments are applied to tables and columns
# MAGIC - Pagination is controlled through `utils_pagination`
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

# MAGIC %run ../99_utils/utils_api_client

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_pagination

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_legislature

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

from datetime import datetime
import json
import uuid

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("02 - BRONZE FRENTES")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "02_bronze_frentes"
LAYER_NAME = "bronze"
ENTITY_NAME = "frentes"

SOURCE_ENDPOINT = API_ENDPOINTS["frentes"]

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["frentes"]
)

LOAD_TYPE = LOAD_TYPE_FULL

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
#
# The /frentes endpoint is sensitive to unsupported query parameters.
#
# Do not use:
# - idLegislatura
# - ordenarPor
# - ordem
#
# This ingestion keeps BASE_PARAMS empty and allows utils_pagination
# to add only supported pagination parameters.
#
# ============================================================

PAGE_SIZE = 15
MAX_PAGES = None

BASE_PARAMS = {}

APPLY_GOVERNANCE_COMMENTS = True

# COMMAND ----------

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
    message="Bronze frentes ingestion started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting frentes ingestion.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract API Records

# COMMAND ----------

try:

    front_records = collect_pages(
        endpoint_path=SOURCE_ENDPOINT,
        base_params=BASE_PARAMS,
        page_size=PAGE_SIZE,
        max_pages=MAX_PAGES,
    )

    records_read = len(front_records)

    log_info(
        pipeline_logger=logger,
        message=(
            f"Parliamentary front records extracted "
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
            f"Failed during frentes API extraction "
            f"| error={str(error)}"
        ),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=None,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Frentes extraction failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Bronze Records

# COMMAND ----------

bronze_rows = []
ingestion_timestamp = datetime.now()

for front_record in front_records:

    raw_json_payload = json.dumps(
        front_record,
        ensure_ascii=False,
    )

    bronze_rows.append({
        "frn_id_frente": str(
            front_record.get("id")
        ),
        "frn_tx_titulo": front_record.get(
            "titulo"
        ),
        "frn_tx_uri": front_record.get(
            "uri"
        ),
        "frn_tx_payload_json": raw_json_payload,
        "aud_id_execucao": execution_id,
        "aud_dh_ingestao": ingestion_timestamp,
        "aud_tx_endpoint_origem": SOURCE_ENDPOINT,
        "aud_tx_sistema_origem": "camara_api",
        "aud_tx_versao_pipeline": PROJECT_VERSION,
        "aud_tx_tipo_carga": LOAD_TYPE,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Bronze DataFrame

# COMMAND ----------

bronze_schema = StructType([
    StructField("frn_id_frente", StringType(), True),
    StructField("frn_tx_titulo", StringType(), True),
    StructField("frn_tx_uri", StringType(), True),
    StructField("frn_tx_payload_json", StringType(), True),
    StructField("aud_id_execucao", StringType(), True),
    StructField("aud_dh_ingestao", TimestampType(), True),
    StructField("aud_tx_endpoint_origem", StringType(), True),
    StructField("aud_tx_sistema_origem", StringType(), True),
    StructField("aud_tx_versao_pipeline", StringType(), True),
    StructField("aud_tx_tipo_carga", StringType(), True),
])

bronze_df = spark.createDataFrame(
    bronze_rows,
    bronze_schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add Record Hash

# COMMAND ----------

bronze_df = add_hash(
    dataframe=bronze_df,
    columns=[
        "frn_id_frente",
        "frn_tx_payload_json",
    ],
    hash_column="aud_tx_hash_registro",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Persist Bronze Table

# COMMAND ----------

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

records_written = records_read

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze frentes table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw parliamentary front ingestion table from Câmara dos Deputados Open Data API.

This Bronze table preserves parliamentary front records extracted from the /frentes endpoint.

Main characteristics:
- raw ingestion fidelity
- original payload preservation
- ingestion metadata
- record hash support
- auditability

Important API note:
- The /frentes endpoint does not accept idLegislatura as a query parameter.
- The /frentes endpoint returned HTTP 400 when using ordenarPor=id and ordem=ASC.
- This ingestion uses only pagination parameters.

Architecture note:
- Legislature association must be handled in downstream layers.
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source endpoint:
- /frentes
"""

column_comments = {
    "frn_id_frente": "Parliamentary front identifier as provided by the Câmara API.",
    "frn_tx_titulo": "Parliamentary front title as provided by the Câmara API.",
    "frn_tx_uri": "Parliamentary front URI as provided by the Câmara API.",
    "frn_tx_payload_json": "Original raw JSON payload preserved from the API response.",
    "aud_id_execucao": "Unique execution identifier for the ingestion run.",
    "aud_dh_ingestao": "Timestamp when the record was ingested into the Bronze layer.",
    "aud_tx_endpoint_origem": "Source API endpoint used to extract the record.",
    "aud_tx_sistema_origem": "Source system name.",
    "aud_tx_versao_pipeline": "Pipeline version responsible for the ingestion.",
    "aud_tx_tipo_carga": "Load type applied during ingestion.",
    "aud_tx_hash_registro": "Deterministic hash used for traceability and deduplication.",
}

if APPLY_GOVERNANCE_COMMENTS:

    apply_table_and_column_comments(
        target_table=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Display Bronze Data

# COMMAND ----------

display(
    bronze_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Final Pipeline Log

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
        f"Bronze frentes ingestion completed successfully "
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
        f"Bronze frentes ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE FRENTES COMPLETED")
print("=" * 90)
print(f"Target Table: {TARGET_TABLE}")
print(f"Source Endpoint: {SOURCE_ENDPOINT}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)