# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Bodies API Ingestion
# MAGIC
# MAGIC **Notebook:** `07_bronze_orgaos`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `/orgaos`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_orgaos`
# MAGIC
# MAGIC Extracts legislative body records from the Câmara dos Deputados
# MAGIC Open Data API and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw API extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Extract legislative body records from the Câmara API
# MAGIC - Use direct API request strategy for institutional reference data
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
# MAGIC - CPI classification is intentionally handled in Silver and Gold layers
# MAGIC - The `/orgaos` endpoint may present instability, intermittent availability and timeout behavior
# MAGIC - API response time may vary significantly depending on Câmara infrastructure conditions
# MAGIC - This notebook intentionally avoids broad historical filters and pagination to reduce timeout risk
# MAGIC - Even with simplified extraction strategy, the endpoint may still present connection timeout instability
# MAGIC - This notebook is recommended mainly for validation, replay and controlled extraction scenarios
# MAGIC - CSV fallback ingestion is the recommended operational strategy for production-scale processing
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - Governance comments are applied to tables and columns
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

# MAGIC  %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Bodies API Ingestion
# MAGIC
# MAGIC **Notebook:** `07_bronze_orgaos`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `/orgaos`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_orgaos`
# MAGIC
# MAGIC Extracts legislative body records from the Câmara dos Deputados
# MAGIC Open Data API and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw API extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Extract legislative body records from the Câmara API
# MAGIC - Use direct API request strategy for institutional reference data
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
# MAGIC - CPI classification is intentionally handled in Silver and Gold layers
# MAGIC - The `/orgaos` endpoint may present timeout behavior with broad filters or pagination
# MAGIC - This notebook avoids broad date filters and pagination to improve execution stability
# MAGIC - This notebook is recommended mainly for validation and controlled extraction scenarios
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - Governance comments are applied to tables and columns
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/governance/data_lineage.md`
# MAGIC - `/docs/governance/data_quality_rules.md`
# MAGIC - `/docs/decisions/api_limitations.md`

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_api_client

# COMMAND ----------

# MAGIC %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

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
print("07 - BRONZE ORGAOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "07_bronze_orgaos"
LAYER_NAME = "bronze"
ENTITY_NAME = "orgaos"

SOURCE_ENDPOINT = API_ENDPOINTS["orgaos"]

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["orgaos"]
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
# The /orgaos endpoint is an institutional reference endpoint.
#
# This notebook avoids pagination and broad date filters because
# the endpoint may present timeout behavior when called with large
# parameter ranges.
#
# ============================================================

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
    message=(
        "Bronze orgaos ingestion started "
        "| extraction_strategy=make_api_request_without_pagination"
    ),
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message=(
        "Starting orgaos ingestion "
        "| extraction_strategy=make_api_request_without_pagination"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract Legislative Bodies

# COMMAND ----------

try:

    response = make_api_request(
        endpoint_path=SOURCE_ENDPOINT,
        params=BASE_PARAMS,
    )

    orgao_records = response.get(
        "dados",
        [],
    )

    records_read = len(orgao_records)

    log_info(
        pipeline_logger=logger,
        message=(
            f"Legislative body records extracted "
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
            f"Failed during orgaos extraction "
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
        message="Orgaos extraction failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Bronze Records

# COMMAND ----------

bronze_rows = []
ingestion_timestamp = datetime.now()

for orgao_record in orgao_records:

    raw_json_payload = json.dumps(
        orgao_record,
        ensure_ascii=False,
    )

    bronze_rows.append({
        "org_id_orgao": str(
            orgao_record.get("id")
        ),
        "org_tx_sigla": orgao_record.get(
            "sigla"
        ),
        "org_tx_nome": orgao_record.get(
            "nome"
        ),
        "org_tx_apelido": orgao_record.get(
            "apelido"
        ),
        "org_tx_tipo_orgao": orgao_record.get(
            "tipoOrgao"
        ),
        "org_tx_sigla_tipo_orgao": orgao_record.get(
            "siglaTipoOrgao"
        ),
        "org_tx_situacao": orgao_record.get(
            "situacao"
        ),
        "org_dt_inicio": str(
            orgao_record.get("dataInicio")
        ),
        "org_dt_fim": str(
            orgao_record.get("dataFim")
        ),
        "org_tx_uri": orgao_record.get(
            "uri"
        ),
        "org_tx_payload_json": raw_json_payload,
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
    StructField("org_id_orgao", StringType(), True),
    StructField("org_tx_sigla", StringType(), True),
    StructField("org_tx_nome", StringType(), True),
    StructField("org_tx_apelido", StringType(), True),
    StructField("org_tx_tipo_orgao", StringType(), True),
    StructField("org_tx_sigla_tipo_orgao", StringType(), True),
    StructField("org_tx_situacao", StringType(), True),
    StructField("org_dt_inicio", StringType(), True),
    StructField("org_dt_fim", StringType(), True),
    StructField("org_tx_uri", StringType(), True),
    StructField("org_tx_payload_json", StringType(), True),
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
        "org_id_orgao",
        "org_tx_payload_json",
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
        f"Bronze orgaos table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Apply Governance Comments

# COMMAND ----------

table_comment = """
Raw legislative bodies ingestion table from Câmara dos Deputados Open Data API.

This Bronze table preserves legislative body records extracted from the /orgaos endpoint.

Main characteristics:
- raw ingestion fidelity
- direct API request strategy without pagination
- legislative body metadata
- original payload preservation
- ingestion metadata
- record hash support
- auditability

Operational note:
- Broad historical date filters were intentionally removed to reduce API timeout risk.
- Pagination was intentionally removed because this endpoint behaves as institutional reference data.
- This notebook is recommended mainly for validation and controlled extraction scenarios.

Architecture decision:
- CPI classification is not applied in Bronze.
- CPI-related entities are derived in Silver/Gold from the complete organization dataset.

Bronze layer note:
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source endpoint:
- /orgaos
"""

column_comments = {
    "org_id_orgao": "Legislative body identifier as provided by the Câmara API.",
    "org_tx_sigla": "Legislative body acronym as provided by the Câmara API.",
    "org_tx_nome": "Legislative body name as provided by the Câmara API.",
    "org_tx_apelido": "Legislative body nickname as provided by the Câmara API.",
    "org_tx_tipo_orgao": "Legislative body type description as provided by the Câmara API.",
    "org_tx_sigla_tipo_orgao": "Legislative body type acronym as provided by the Câmara API.",
    "org_tx_situacao": "Legislative body status as provided by the Câmara API.",
    "org_dt_inicio": "Legislative body start date as provided by the Câmara API.",
    "org_dt_fim": "Legislative body end date as provided by the Câmara API.",
    "org_tx_uri": "Legislative body URI as provided by the Câmara API.",
    "org_tx_payload_json": "Original raw JSON payload preserved from the API response.",
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
        f"Bronze orgaos ingestion completed successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| extraction_strategy=make_api_request_without_pagination"
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
        f"Bronze orgaos ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE ORGAOS COMPLETED")
print("=" * 90)
print(f"Target Table: {TARGET_TABLE}")
print(f"Source Endpoint: {SOURCE_ENDPOINT}")
print("Extraction Strategy: make_api_request_without_pagination")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)