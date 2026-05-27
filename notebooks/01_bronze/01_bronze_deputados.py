# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Deputies API Ingestion
# MAGIC
# MAGIC **Notebook:** `01_bronze_deputados`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `/deputados`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_deputados`
# MAGIC
# MAGIC Extracts deputy records from the Câmara dos Deputados Open Data API
# MAGIC and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw API extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Extract deputy records from the Câmara API
# MAGIC - Filter records by supported legislatures
# MAGIC - Preserve raw API payloads
# MAGIC - Generate ingestion traceability metadata
# MAGIC - Generate deterministic record hashes
# MAGIC - Persist Bronze Delta tables
# MAGIC - Register operational execution logs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves source-system extraction fidelity
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - Supports legislature-scoped extraction strategy
# MAGIC - Governance comments can be optionally applied to tables and columns
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/governance/data_lineage.md`
# MAGIC - `/docs/governance/data_quality_rules.md`

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_api_client

# COMMAND ----------

# MAGIC %run ../99_utils/utils_pagination

# COMMAND ----------

# MAGIC %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_legislature

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
print("01 - BRONZE DEPUTADOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

NOTEBOOK_NAME = "01_bronze_deputados"
LAYER_NAME = "bronze"
ENTITY_NAME = "deputados"

SOURCE_ENDPOINT = API_ENDPOINTS["deputados"]

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["deputados"]
)

LOAD_TYPE = LOAD_TYPE_FULL

execution_id = str(uuid.uuid4())
started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

# COMMAND ----------

PAGE_SIZE = 100
MAX_PAGES = None

REFERENCE_LEGISLATURES = get_supported_legislatures()

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
    message="Bronze deputados ingestion started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message=(
        f"Starting deputados extraction "
        f"| legislatures={REFERENCE_LEGISLATURES}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract API Records by Legislature

# COMMAND ----------

try:

    deputy_records = []

    for legislature_id in REFERENCE_LEGISLATURES:

        log_info(
            pipeline_logger=logger,
            message=(
                f"Starting deputies extraction by legislature "
                f"| legislature_id={legislature_id}"
            ),
        )

        legislature_records = collect_pages(
            endpoint_path=SOURCE_ENDPOINT,
            base_params={
                "ordem": "ASC",
                "ordenarPor": "id",
                "idLegislatura": legislature_id,
            },
            page_size=PAGE_SIZE,
            max_pages=MAX_PAGES,
        )

        for record in legislature_records:
            record["idLegislaturaReferencia"] = legislature_id

        deputy_records.extend(legislature_records)

        log_info(
            pipeline_logger=logger,
            message=(
                f"Deputies extracted by legislature "
                f"| legislature_id={legislature_id} "
                f"| records={len(legislature_records)}"
            ),
        )

    records_read = len(deputy_records)

    if records_read == 0:
        raise Exception("No deputy records were extracted from API.")

    log_info(
        pipeline_logger=logger,
        message=(
            f"Deputy records extracted successfully "
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
            f"Failed during deputados extraction "
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
        message="Deputados extraction failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Bronze Records

# COMMAND ----------

bronze_rows = []
ingestion_timestamp = datetime.now()

for deputy_record in deputy_records:

    raw_json_payload = json.dumps(
        deputy_record,
        ensure_ascii=False,
    )

    bronze_rows.append({
        "dep_id_deputado": str(deputy_record.get("id")),
        "dep_tx_uri": deputy_record.get("uri"),
        "dep_tx_nome": deputy_record.get("nome"),
        "dep_tx_sigla_partido": deputy_record.get("siglaPartido"),
        "dep_tx_uri_partido": deputy_record.get("uriPartido"),
        "dep_tx_sigla_uf": deputy_record.get("siglaUf"),
        "dep_id_legislatura": str(deputy_record.get("idLegislatura")),
        "dep_id_legislatura_referencia": str(
            deputy_record.get("idLegislaturaReferencia")
        ),
        "dep_tx_url_foto": deputy_record.get("urlFoto"),
        "dep_tx_email": deputy_record.get("email"),
        "dep_tx_payload_json": raw_json_payload,
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
    StructField("dep_id_deputado", StringType(), True),
    StructField("dep_tx_uri", StringType(), True),
    StructField("dep_tx_nome", StringType(), True),
    StructField("dep_tx_sigla_partido", StringType(), True),
    StructField("dep_tx_uri_partido", StringType(), True),
    StructField("dep_tx_sigla_uf", StringType(), True),
    StructField("dep_id_legislatura", StringType(), True),
    StructField("dep_id_legislatura_referencia", StringType(), True),
    StructField("dep_tx_url_foto", StringType(), True),
    StructField("dep_tx_email", StringType(), True),
    StructField("dep_tx_payload_json", StringType(), True),
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
        "dep_id_deputado",
        "dep_id_legislatura_referencia",
        "dep_tx_payload_json",
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
        f"Bronze deputados table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Apply Governance Comments

# COMMAND ----------

table_comment = """
Raw deputy ingestion table from Câmara dos Deputados Open Data API.

This Bronze table preserves deputy records extracted from the /deputados endpoint.

Main characteristics:
- raw API ingestion fidelity
- legislature-scoped extraction
- deputy metadata
- original payload preservation
- ingestion metadata
- record hash support
- auditability

Bronze layer note:
- Technical duplicates, standardization rules and business validation are intentionally handled in the Silver layer.

Source endpoint:
- /deputados
"""

column_comments = {
    "dep_id_deputado": "Deputy identifier as provided by the Câmara API.",
    "dep_tx_uri": "Deputy URI as provided by the Câmara API.",
    "dep_tx_nome": "Deputy name as provided by the Câmara API.",
    "dep_tx_sigla_partido": "Deputy political party acronym.",
    "dep_tx_uri_partido": "Deputy political party URI.",
    "dep_tx_sigla_uf": "Deputy Brazilian state acronym.",
    "dep_id_legislatura": "Legislature identifier returned by the API.",
    "dep_id_legislatura_referencia": "Reference legislature used as extraction filter.",
    "dep_tx_url_foto": "Deputy photo URL.",
    "dep_tx_email": "Deputy institutional email address.",
    "dep_tx_payload_json": "Original raw JSON payload preserved from the API response.",
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
# MAGIC ## 8. Display Bronze Data Sample

# COMMAND ----------

print(f"Bronze deputados table available: {TARGET_TABLE}")
print(f"Records written: {records_written}")

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
        f"Bronze deputados ingestion completed successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| legislatures={REFERENCE_LEGISLATURES}"
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
        f"Bronze deputados ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE DEPUTADOS COMPLETED")
print("=" * 90)
print(f"Target Table: {TARGET_TABLE}")
print(f"Reference Legislatures: {REFERENCE_LEGISLATURES}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)