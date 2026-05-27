# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — CEAP Expenses API Ingestion
# MAGIC
# MAGIC **Notebook:** `06_bronze_despesas_ceap`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `/deputados/{id}/despesas`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_despesas_ceap`
# MAGIC
# MAGIC Extracts CEAP expense records from the Câmara dos Deputados
# MAGIC Open Data API and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw API extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Load deputy identifiers from Bronze deputy tables
# MAGIC - Extract CEAP expense records from the Câmara API
# MAGIC - Handle deputy-based, year-based and paginated API extraction
# MAGIC - Preserve raw API payloads
# MAGIC - Generate ingestion traceability metadata
# MAGIC - Generate deterministic record hashes
# MAGIC - Persist Bronze Delta tables
# MAGIC - Register operational execution logs
# MAGIC - Monitor ingestion progress and failed requests
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves source-system extraction fidelity
# MAGIC - CEAP extraction scope is controlled through `CEAP_REFERENCE_YEARS`
# MAGIC - API ingestion is recommended mainly for validation and controlled extraction scenarios
# MAGIC - CSV fallback ingestion is recommended for high-volume operational processing
# MAGIC - Includes request monitoring, progress logging and slow request detection capabilities
# MAGIC - The `/deputados/{id}/despesas` endpoint may present timeout and pagination instability behavior
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - Governance comments are applied to tables and columns
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/decisions/api_limitations.md`
# MAGIC - `/docs/monitoring/observability.md`

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
import time

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("06 - BRONZE DESPESAS CEAP API")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "06_bronze_despesas_ceap"
LAYER_NAME = "bronze"
ENTITY_NAME = "despesas_ceap"

SOURCE_ENDPOINT_TEMPLATE = API_ENDPOINTS["despesas_ceap"]

SOURCE_DEPUTY_TABLE = get_bronze_table(
    BRONZE_TABLES["deputados"]
)

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["despesas_ceap"]
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
# CEAP API extraction is intentionally controlled because this endpoint
# requires requests by deputy and by year.
#
# The CSV fallback notebook remains the recommended strategy for
# high-volume operational processing.
#
# This API notebook is mainly recommended for:
# - endpoint validation
# - controlled samples
# - replay tests
# - compatibility checks
#
# Reference years are centralized in 00_setup/01_project_config.
#
# ============================================================

PAGE_SIZE = 50
MAX_PAGES = 2

MAX_DEPUTIES = 5

EXPENSE_YEARS = CEAP_REFERENCE_YEARS

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRY_ATTEMPTS = 2
SLEEP_BETWEEN_REQUESTS_SECONDS = 0.5

SLOW_REQUEST_THRESHOLD_SECONDS = 15
PROGRESS_LOG_INTERVAL = 2

BASE_PARAMS = {
    "ordem": "ASC",
    "ordenarPor": "ano",
}

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

def log_progress(
    current_step: int,
    total_steps: int,
    records_collected: int,
    failed_requests: int,
    execution_start_time: datetime,
) -> None:
    """
    Logs ingestion progress and elapsed execution time.
    """

    elapsed_seconds = (
        datetime.now() - execution_start_time
    ).total_seconds()

    log_info(
        pipeline_logger=logger,
        message=(
            f"CEAP ingestion progress "
            f"| step={current_step}/{total_steps} "
            f"| records_collected={records_collected} "
            f"| failed_requests={failed_requests} "
            f"| elapsed_seconds={round(elapsed_seconds, 2)}"
        ),
    )

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
        f"Bronze despesas CEAP API ingestion started "
        f"| expense_years={EXPENSE_YEARS} "
        f"| max_deputies={MAX_DEPUTIES} "
        f"| max_pages={MAX_PAGES}"
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
        f"Starting despesas CEAP API ingestion "
        f"| expense_years={EXPENSE_YEARS} "
        f"| max_deputies={MAX_DEPUTIES} "
        f"| max_pages={MAX_PAGES}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Deputy Identifiers from Bronze Deputados

# COMMAND ----------

try:

    deputy_ids_df = (
        spark.table(SOURCE_DEPUTY_TABLE)
        .select("dep_id_deputado")
        .where("dep_id_deputado IS NOT NULL")
        .dropDuplicates()
        .orderBy("dep_id_deputado")
    )

    if MAX_DEPUTIES is not None:
        deputy_ids_df = deputy_ids_df.limit(MAX_DEPUTIES)

    deputy_ids = [
        row["dep_id_deputado"]
        for row in deputy_ids_df.collect()
    ]

    log_info(
        pipeline_logger=logger,
        message=(
            f"Deputy identifiers loaded for CEAP extraction "
            f"| total_deputies={len(deputy_ids)}"
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
            f"Failed loading deputy identifiers "
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
        message="Failed loading deputy identifiers.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extract CEAP Expense Records

# COMMAND ----------

try:

    expense_records = []
    failed_requests = []

    total_steps = len(deputy_ids) * len(EXPENSE_YEARS)
    current_step = 0

    for deputy_index, deputy_id in enumerate(
        deputy_ids,
        start=1,
    ):

        deputy_started_at = datetime.now()

        log_info(
            pipeline_logger=logger,
            message=(
                f"Starting deputy CEAP extraction "
                f"| deputy_id={deputy_id} "
                f"| deputy_position={deputy_index}/{len(deputy_ids)}"
            ),
        )

        for expense_year in EXPENSE_YEARS:

            current_step += 1
            request_started_at = datetime.now()

            endpoint_path = SOURCE_ENDPOINT_TEMPLATE.replace(
                "{id}",
                str(deputy_id),
            )

            year_params = dict(BASE_PARAMS)
            year_params["ano"] = expense_year

            log_info(
                pipeline_logger=logger,
                message=(
                    f"Starting CEAP request "
                    f"| deputy_id={deputy_id} "
                    f"| year={expense_year} "
                    f"| step={current_step}/{total_steps}"
                ),
            )

            try:

                deputy_year_records = collect_pages(
                    endpoint_path=endpoint_path,
                    base_params=year_params,
                    page_size=PAGE_SIZE,
                    max_pages=MAX_PAGES,
                    request_timeout=REQUEST_TIMEOUT_SECONDS,
                    max_retries=MAX_RETRY_ATTEMPTS,
                    sleep_seconds=0.5,
                )

                for record in deputy_year_records:
                    record["idDeputado"] = deputy_id
                    record["anoExtracao"] = expense_year

                expense_records.extend(
                    deputy_year_records
                )

                request_duration_seconds = (
                    datetime.now() - request_started_at
                ).total_seconds()

                if (
                    request_duration_seconds
                    >= SLOW_REQUEST_THRESHOLD_SECONDS
                ):

                    log_warning(
                        pipeline_logger=logger,
                        message=(
                            f"Slow CEAP request detected "
                            f"| deputy_id={deputy_id} "
                            f"| year={expense_year} "
                            f"| duration_seconds={round(request_duration_seconds, 2)} "
                            f"| records={len(deputy_year_records)}"
                        ),
                    )

                else:

                    log_info(
                        pipeline_logger=logger,
                        message=(
                            f"CEAP request completed "
                            f"| deputy_id={deputy_id} "
                            f"| year={expense_year} "
                            f"| duration_seconds={round(request_duration_seconds, 2)} "
                            f"| records={len(deputy_year_records)}"
                        ),
                    )

            except Exception as request_error:

                failed_requests.append({
                    "deputy_id": deputy_id,
                    "year": expense_year,
                    "error": str(request_error),
                })

                log_warning(
                    pipeline_logger=logger,
                    message=(
                        f"CEAP request failed "
                        f"| deputy_id={deputy_id} "
                        f"| year={expense_year} "
                        f"| error={str(request_error)}"
                    ),
                )

            if (
                current_step % PROGRESS_LOG_INTERVAL == 0
                or current_step == total_steps
            ):

                log_progress(
                    current_step=current_step,
                    total_steps=total_steps,
                    records_collected=len(expense_records),
                    failed_requests=len(failed_requests),
                    execution_start_time=started_at,
                )

            if SLEEP_BETWEEN_REQUESTS_SECONDS > 0:

                time.sleep(
                    SLEEP_BETWEEN_REQUESTS_SECONDS
                )

        deputy_duration_seconds = (
            datetime.now() - deputy_started_at
        ).total_seconds()

        log_info(
            pipeline_logger=logger,
            message=(
                f"Deputy CEAP extraction completed "
                f"| deputy_id={deputy_id} "
                f"| duration_seconds={round(deputy_duration_seconds, 2)}"
            ),
        )

    records_read = len(expense_records)

    log_info(
        pipeline_logger=logger,
        message=(
            f"CEAP expense records extracted "
            f"| records_read={records_read} "
            f"| failed_requests={len(failed_requests)}"
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
            f"Failed during CEAP extraction "
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
        message="Despesas CEAP extraction failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Bronze Records

# COMMAND ----------

bronze_rows = []
ingestion_timestamp = datetime.now()

for expense_record in expense_records:

    raw_json_payload = json.dumps(
        expense_record,
        ensure_ascii=False,
    )

    bronze_rows.append({
        "dep_id_deputado": str(
            expense_record.get("idDeputado")
        ),
        "desp_nr_ano": str(
            expense_record.get("ano")
            or expense_record.get("anoExtracao")
        ),
        "desp_nr_mes": str(
            expense_record.get("mes")
        ),
        "desp_tx_tipo_despesa": expense_record.get(
            "tipoDespesa"
        ),
        "desp_tx_tipo_documento": expense_record.get(
            "tipoDocumento"
        ),
        "desp_tx_numero_documento": expense_record.get(
            "numDocumento"
        ),
        "desp_tx_nome_fornecedor": expense_record.get(
            "nomeFornecedor"
        ),
        "desp_tx_cnpj_cpf_fornecedor": expense_record.get(
            "cnpjCpfFornecedor"
        ),
        "desp_dt_data_documento": str(
            expense_record.get("dataDocumento")
        ),
        "desp_vl_documento": str(
            expense_record.get("valorDocumento")
        ),
        "desp_vl_glosa": str(
            expense_record.get("valorGlosa")
        ),
        "desp_vl_liquido": str(
            expense_record.get("valorLiquido")
        ),
        "desp_tx_url_documento": expense_record.get(
            "urlDocumento"
        ),
        "desp_tx_payload_json": raw_json_payload,
        "aud_id_execucao": execution_id,
        "aud_dh_ingestao": ingestion_timestamp,
        "aud_tx_endpoint_origem": SOURCE_ENDPOINT_TEMPLATE,
        "aud_tx_sistema_origem": "camara_api",
        "aud_tx_versao_pipeline": PROJECT_VERSION,
        "aud_tx_tipo_carga": LOAD_TYPE,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Bronze DataFrame

# COMMAND ----------

bronze_schema = StructType([
    StructField("dep_id_deputado", StringType(), True),
    StructField("desp_nr_ano", StringType(), True),
    StructField("desp_nr_mes", StringType(), True),
    StructField("desp_tx_tipo_despesa", StringType(), True),
    StructField("desp_tx_tipo_documento", StringType(), True),
    StructField("desp_tx_numero_documento", StringType(), True),
    StructField("desp_tx_nome_fornecedor", StringType(), True),
    StructField("desp_tx_cnpj_cpf_fornecedor", StringType(), True),
    StructField("desp_dt_data_documento", StringType(), True),
    StructField("desp_vl_documento", StringType(), True),
    StructField("desp_vl_glosa", StringType(), True),
    StructField("desp_vl_liquido", StringType(), True),
    StructField("desp_tx_url_documento", StringType(), True),
    StructField("desp_tx_payload_json", StringType(), True),
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
# MAGIC ## 6. Add Record Hash

# COMMAND ----------

bronze_df = add_hash(
    dataframe=bronze_df,
    columns=[
        "dep_id_deputado",
        "desp_nr_ano",
        "desp_nr_mes",
        "desp_tx_numero_documento",
        "desp_tx_payload_json",
    ],
    hash_column="aud_tx_hash_registro",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Persist Bronze Table

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
        f"Bronze despesas CEAP API table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw CEAP expense ingestion table from Câmara dos Deputados Open Data API.

This Bronze table preserves CEAP expense records extracted from the /deputados/{id}/despesas endpoint.

Main characteristics:
- raw ingestion fidelity
- deputy-level extraction
- centralized reference year configuration
- controlled API sampling
- original payload preservation
- ingestion metadata
- record hash support
- auditability

Monitoring characteristics:
- progress logging by deputy and year
- slow request detection
- failed request tracking
- execution duration visibility

Operational note:
- API ingestion is recommended mainly for validation and controlled extraction scenarios.
- CSV fallback ingestion remains the preferred strategy for high-volume operational processing.

Bronze layer note:
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source endpoint:
- /deputados/{id}/despesas
"""

column_comments = {
    "dep_id_deputado": "Deputy identifier associated with the CEAP expense record.",
    "desp_nr_ano": "Expense year as provided by the API or extraction parameter.",
    "desp_nr_mes": "Expense month as provided by the API.",
    "desp_tx_tipo_despesa": "Expense type description as provided by the Câmara API.",
    "desp_tx_tipo_documento": "Expense document type as provided by the Câmara API.",
    "desp_tx_numero_documento": "Expense document number as provided by the Câmara API.",
    "desp_tx_nome_fornecedor": "Supplier name as provided by the Câmara API.",
    "desp_tx_cnpj_cpf_fornecedor": "Supplier CNPJ or CPF as provided by the Câmara API.",
    "desp_dt_data_documento": "Expense document date as provided by the Câmara API.",
    "desp_vl_documento": "Expense document value as provided by the Câmara API.",
    "desp_vl_glosa": "Expense disallowed amount as provided by the Câmara API.",
    "desp_vl_liquido": "Expense net amount as provided by the Câmara API.",
    "desp_tx_url_documento": "URL of the expense document as provided by the Câmara API.",
    "desp_tx_payload_json": "Original raw JSON payload preserved from the API response.",
    "aud_id_execucao": "Unique execution identifier for the ingestion run.",
    "aud_dh_ingestao": "Timestamp when the record was ingested into the Bronze layer.",
    "aud_tx_endpoint_origem": "Source API endpoint template used to extract the record.",
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
# MAGIC ## 9. Display Bronze Data

# COMMAND ----------

display(
    bronze_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Final Pipeline Log

# COMMAND ----------

finished_at = datetime.now()

duration_seconds = (
    finished_at - started_at
).total_seconds()

status = (
    EXECUTION_STATUS_WARNING
    if len(failed_requests) > 0
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
        f"Bronze despesas CEAP API ingestion completed "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| failed_requests={len(failed_requests)} "
        f"| expense_years={EXPENSE_YEARS}"
    ),
    started_at=started_at,
    finished_at=finished_at,
    duration_seconds=duration_seconds,
    records_read=records_read,
    records_written=records_written,
)

if len(failed_requests) > 0:

    log_warning(
        pipeline_logger=logger,
        message=(
            f"Bronze despesas CEAP API completed with failed requests "
            f"| failed_requests={len(failed_requests)}"
        ),
    )

else:

    log_success(
        pipeline_logger=logger,
        message=(
            f"Bronze despesas CEAP API ingestion completed "
            f"| duration_seconds={duration_seconds}"
        ),
    )

# COMMAND ----------

print("=" * 90)
print("BRONZE DESPESAS CEAP API COMPLETED")
print("=" * 90)
print(f"Target Table: {TARGET_TABLE}")
print(f"Deputies Processed: {len(deputy_ids)}")
print(f"Reference Years: {EXPENSE_YEARS}")
print(f"Max Deputies: {MAX_DEPUTIES}")
print(f"Max Pages: {MAX_PAGES}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Failed Requests: {len(failed_requests)}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)