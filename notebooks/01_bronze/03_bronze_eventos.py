# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Events API Ingestion
# MAGIC
# MAGIC **Notebook:** `03_bronze_eventos`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `/eventos`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_eventos`
# MAGIC
# MAGIC Extracts legislative event records from the Câmara dos Deputados
# MAGIC Open Data API and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook uses daily extraction windows by reference year
# MAGIC to reduce API timeout risk and improve ingestion reliability.
# MAGIC
# MAGIC Raw API extraction fidelity is preserved,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Extract legislative event records from the Câmara API
# MAGIC - Retrieve events using daily extraction windows
# MAGIC - Handle paginated API ingestion
# MAGIC - Preserve raw API payloads
# MAGIC - Generate ingestion traceability metadata
# MAGIC - Generate deterministic record hashes
# MAGIC - Persist Bronze Delta tables
# MAGIC - Register operational execution logs
# MAGIC - Support partial recovery by extraction window
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves source-system extraction fidelity
# MAGIC - Event extraction scope is controlled through `EVENTOS_REFERENCE_YEARS`
# MAGIC - Daily extraction windows reduce API timeout risk
# MAGIC - The `/eventos` endpoint may become unstable during broad extraction requests
# MAGIC - Includes failed window tracking and operational progress logging
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - Governance comments are applied to tables and columns
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

# MAGIC  %run ../99_utils/utils_pagination

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

from datetime import datetime, timedelta
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
print("03 - BRONZE EVENTOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "03_bronze_eventos"
LAYER_NAME = "bronze"
ENTITY_NAME = "eventos"

SOURCE_ENDPOINT = API_ENDPOINTS["eventos"]

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["eventos"]
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

PAGE_SIZE = 25
MAX_PAGES = None

REQUEST_TIMEOUT_SECONDS = 120
MAX_RETRY_ATTEMPTS = 3
SLEEP_SECONDS = 0.5

REFERENCE_YEARS = EVENTOS_REFERENCE_YEARS

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
        f"Bronze eventos ingestion started "
        f"| reference_years={REFERENCE_YEARS}"
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
        f"Starting eventos ingestion "
        f"| reference_years={REFERENCE_YEARS} "
        f"| extraction_granularity=daily"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract API Records by Daily Windows

# COMMAND ----------

try:

    event_records = []
    failed_windows = []

    for reference_year in REFERENCE_YEARS:

        window_start = datetime(reference_year, 1, 1)
        window_end = datetime(reference_year, 12, 31)

        while window_start <= window_end:

            window_date = window_start.strftime("%Y-%m-%d")

            try:

                daily_records = collect_pages(
                    endpoint_path=SOURCE_ENDPOINT,
                    base_params={
                        "dataInicio": window_date,
                        "dataFim": window_date,
                    },
                    page_size=PAGE_SIZE,
                    max_pages=MAX_PAGES,
                    request_timeout=REQUEST_TIMEOUT_SECONDS,
                    max_retries=MAX_RETRY_ATTEMPTS,
                    sleep_seconds=SLEEP_SECONDS,
                )

                for record in daily_records:
                    record["anoReferencia"] = reference_year
                    record["dataInicioJanela"] = window_date
                    record["dataFimJanela"] = window_date

                event_records.extend(daily_records)

                log_info(
                    pipeline_logger=logger,
                    message=(
                        f"Eventos extraction window completed "
                        f"| reference_year={reference_year} "
                        f"| window_date={window_date} "
                        f"| records={len(daily_records)}"
                    ),
                )

            except Exception as window_error:

                failed_windows.append({
                    "reference_year": reference_year,
                    "window_date": window_date,
                    "error": str(window_error),
                })

                log_warning(
                    pipeline_logger=logger,
                    message=(
                        f"Eventos extraction window failed "
                        f"| reference_year={reference_year} "
                        f"| window_date={window_date} "
                        f"| error={str(window_error)}"
                    ),
                )

            window_start = window_start + timedelta(days=1)

    records_read = len(event_records)

    log_info(
        pipeline_logger=logger,
        message=(
            f"Legislative event records extracted "
            f"| records_read={records_read} "
            f"| failed_windows={len(failed_windows)}"
        ),
    )

except Exception as error:

    finished_at = datetime.now()
    duration_seconds = (finished_at - started_at).total_seconds()

    write_pipeline_log(
        log_id=str(uuid.uuid4()),
        execution_id=execution_id,
        notebook_name=NOTEBOOK_NAME,
        layer_name=LAYER_NAME,
        entity_name=ENTITY_NAME,
        target_table=TARGET_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=f"Failed during eventos extraction | error={str(error)}",
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=None,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Eventos extraction failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Bronze Records

# COMMAND ----------

bronze_rows = []
ingestion_timestamp = datetime.now()

for event_record in event_records:

    raw_json_payload = json.dumps(
        event_record,
        ensure_ascii=False,
    )

    bronze_rows.append({
        "evt_id_evento": str(event_record.get("id")),
        "evt_tx_descricao": event_record.get("descricao"),
        "evt_tx_local": event_record.get("localCamara"),
        "evt_dt_data_hora_inicio": str(event_record.get("dataHoraInicio")),
        "evt_dt_data_hora_fim": str(event_record.get("dataHoraFim")),
        "evt_tx_situacao": event_record.get("situacao"),
        "evt_tx_uri": event_record.get("uri"),
        "evt_nr_ano_referencia": str(event_record.get("anoReferencia")),
        "evt_dt_inicio_janela": event_record.get("dataInicioJanela"),
        "evt_dt_fim_janela": event_record.get("dataFimJanela"),
        "evt_tx_payload_json": raw_json_payload,
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
    StructField("evt_id_evento", StringType(), True),
    StructField("evt_tx_descricao", StringType(), True),
    StructField("evt_tx_local", StringType(), True),
    StructField("evt_dt_data_hora_inicio", StringType(), True),
    StructField("evt_dt_data_hora_fim", StringType(), True),
    StructField("evt_tx_situacao", StringType(), True),
    StructField("evt_tx_uri", StringType(), True),
    StructField("evt_nr_ano_referencia", StringType(), True),
    StructField("evt_dt_inicio_janela", StringType(), True),
    StructField("evt_dt_fim_janela", StringType(), True),
    StructField("evt_tx_payload_json", StringType(), True),
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
        "evt_id_evento",
        "evt_nr_ano_referencia",
        "evt_dt_inicio_janela",
        "evt_dt_fim_janela",
        "evt_tx_payload_json",
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

records_written = bronze_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze eventos table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Apply Governance Comments

# COMMAND ----------

table_comment = """
Raw legislative events ingestion table from Câmara dos Deputados Open Data API.

This Bronze table preserves legislative event records extracted from the /eventos endpoint.

Main characteristics:
- raw ingestion fidelity
- daily extraction window strategy
- original payload preservation
- ingestion metadata
- record hash support
- auditability
- replay support by reference year and extraction window

Operational note:
- The /eventos endpoint may timeout when queried broadly.
- Daily windows reduce API response size and improve ingestion reliability.

Bronze layer note:
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source endpoint:
- /eventos
"""

column_comments = {
    "evt_id_evento": "Legislative event identifier as provided by the Câmara API.",
    "evt_tx_descricao": "Legislative event description as provided by the Câmara API.",
    "evt_tx_local": "Legislative event location as provided by the Câmara API.",
    "evt_dt_data_hora_inicio": "Event start datetime as provided by the Câmara API.",
    "evt_dt_data_hora_fim": "Event end datetime as provided by the Câmara API.",
    "evt_tx_situacao": "Event status as provided by the Câmara API.",
    "evt_tx_uri": "Legislative event URI as provided by the Câmara API.",
    "evt_nr_ano_referencia": "Reference year used during daily extraction.",
    "evt_dt_inicio_janela": "Extraction window start date used in the API request.",
    "evt_dt_fim_janela": "Extraction window end date used in the API request.",
    "evt_tx_payload_json": "Original raw JSON payload preserved from the API response.",
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

status = (
    EXECUTION_STATUS_WARNING
    if len(failed_windows) > 0
    else EXECUTION_STATUS_SUCCESS
)

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=status,
    message=(
        f"Bronze eventos ingestion completed "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| failed_windows={len(failed_windows)} "
        f"| reference_years={REFERENCE_YEARS}"
    ),
    started_at=started_at,
    finished_at=finished_at,
    duration_seconds=duration_seconds,
    records_read=records_read,
    records_written=records_written,
)

if len(failed_windows) > 0:

    log_warning(
        pipeline_logger=logger,
        message=(
            f"Bronze eventos completed with failed extraction windows "
            f"| failed_windows={len(failed_windows)}"
        ),
    )

else:

    log_success(
        pipeline_logger=logger,
        message=(
            f"Bronze eventos ingestion completed "
            f"| duration_seconds={duration_seconds}"
        ),
    )

# COMMAND ----------

print("=" * 90)
print("BRONZE EVENTOS COMPLETED")
print("=" * 90)
print(f"Target Table: {TARGET_TABLE}")
print(f"Reference Years: {REFERENCE_YEARS}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Failed Windows: {len(failed_windows)}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)

# COMMAND ----------

# MAGIC %md
# MAGIC