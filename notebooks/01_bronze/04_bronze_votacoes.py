# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Voting Sessions API Ingestion
# MAGIC
# MAGIC **Notebook:** `04_bronze_votacoes`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `/votacoes`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_votacoes`
# MAGIC
# MAGIC Extracts voting session records from the Câmara dos Deputados
# MAGIC Open Data API and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw API extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Extract voting session records from the Câmara API
# MAGIC - Handle year-based and paginated API ingestion
# MAGIC - Preserve raw API payloads
# MAGIC - Generate ingestion traceability metadata
# MAGIC - Generate deterministic record hashes
# MAGIC - Persist Bronze Delta tables
# MAGIC - Register operational execution logs
# MAGIC - Support partial recovery by reference year
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves source-system extraction fidelity
# MAGIC - Voting extraction scope is controlled through `DEFAULT_REFERENCE_YEARS`
# MAGIC - The `/votacoes` endpoint may become unstable during deep pagination
# MAGIC - This notebook is recommended for validation and controlled API ingestion scenarios
# MAGIC - CSV fallback ingestion is recommended for high-volume operational processing
# MAGIC - Governance comments are applied to tables and columns
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/decisions/api_limitations.md`
# MAGIC - `/docs/governance/data_lineage.md`

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_api_client

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_legislature

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_pagination

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
print("04 - BRONZE VOTACOES API")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "04_bronze_votacoes"
LAYER_NAME = "bronze"
ENTITY_NAME = "votacoes"

SOURCE_ENDPOINT = API_ENDPOINTS["votacoes"]

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["votacoes"]
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
# The /votacoes endpoint may become unstable during deep pagination.
#
# This notebook uses year-based extraction to improve:
# - operational control
# - replay capability
# - pagination stability
# - partial recovery
#
# For high-volume operational ingestion,
# the CSV fallback notebook remains the preferred strategy.
#
# ============================================================

PAGE_SIZE = 100

MAX_PAGES = 10

REFERENCE_YEARS = DEFAULT_REFERENCE_YEARS

REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRY_ATTEMPTS = 3
SLEEP_BETWEEN_YEARS_SECONDS = 0.5

BASE_PARAMS = {
    "ordem": "ASC",
    "ordenarPor": "id",
}

APPLY_GOVERNANCE_COMMENTS = True

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
    message=(
        f"Bronze votacoes API ingestion started "
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
        f"Starting votacoes API ingestion "
        f"| reference_years={REFERENCE_YEARS}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract API Records by Year

# COMMAND ----------

try:

    voting_records = []
    failed_years = []

    for reference_year in REFERENCE_YEARS:

        year_params = dict(BASE_PARAMS)
        year_params["ano"] = reference_year

        log_info(
            pipeline_logger=logger,
            message=(
                f"Starting votacoes extraction "
                f"| reference_year={reference_year}"
            ),
        )

        try:

            year_records = collect_pages(
                endpoint_path=SOURCE_ENDPOINT,
                base_params=year_params,
                page_size=PAGE_SIZE,
                max_pages=MAX_PAGES,
                request_timeout=REQUEST_TIMEOUT_SECONDS,
                max_retries=MAX_RETRY_ATTEMPTS,
                sleep_seconds=1,
            )

            for record in year_records:
                record["anoReferencia"] = reference_year

            voting_records.extend(
                year_records
            )

            log_info(
                pipeline_logger=logger,
                message=(
                    f"Voting records extracted "
                    f"| reference_year={reference_year} "
                    f"| records={len(year_records)}"
                ),
            )

        except Exception as year_error:

            failed_years.append({
                "reference_year": reference_year,
                "error": str(year_error),
            })

            log_warning(
                pipeline_logger=logger,
                message=(
                    f"Voting extraction failed "
                    f"| reference_year={reference_year} "
                    f"| error={str(year_error)}"
                ),
            )

        if SLEEP_BETWEEN_YEARS_SECONDS > 0:

            time.sleep(
                SLEEP_BETWEEN_YEARS_SECONDS
            )

    records_read = len(voting_records)

    log_info(
        pipeline_logger=logger,
        message=(
            f"Voting records extracted "
            f"| records_read={records_read} "
            f"| failed_years={len(failed_years)}"
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
            f"Failed during votacoes API extraction "
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
        message="Votacoes API extraction failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Bronze Records

# COMMAND ----------

bronze_rows = []

ingestion_timestamp = datetime.now()

for voting_record in voting_records:

    raw_json_payload = json.dumps(
        voting_record,
        ensure_ascii=False,
    )

    bronze_rows.append({
        "vot_id_votacao": str(
            voting_record.get("id")
        ),
        "vot_tx_uri": voting_record.get(
            "uri"
        ),
        "vot_tx_descricao": voting_record.get(
            "descricao"
        ),
        "vot_dt_data_hora_registro": str(
            voting_record.get("dataHoraRegistro")
        ),
        "vot_tx_sigla_orgao": voting_record.get(
            "siglaOrgao"
        ),
        "vot_tx_aprovacao": str(
            voting_record.get("aprovacao")
        ),
        "vot_nr_ano_referencia": str(
            voting_record.get("anoReferencia")
        ),
        "vot_tx_payload_json": raw_json_payload,
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
    StructField("vot_id_votacao", StringType(), True),
    StructField("vot_tx_uri", StringType(), True),
    StructField("vot_tx_descricao", StringType(), True),
    StructField("vot_dt_data_hora_registro", StringType(), True),
    StructField("vot_tx_sigla_orgao", StringType(), True),
    StructField("vot_tx_aprovacao", StringType(), True),
    StructField("vot_nr_ano_referencia", StringType(), True),
    StructField("vot_tx_payload_json", StringType(), True),
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
        "vot_id_votacao",
        "vot_nr_ano_referencia",
        "vot_tx_payload_json",
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
        f"Bronze votacoes API table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw voting session ingestion table from Câmara dos Deputados Open Data API.

This Bronze table preserves voting records extracted from the /votacoes endpoint.

Main characteristics:
- raw API ingestion fidelity
- year-based extraction strategy
- centralized reference year configuration
- original payload preservation
- ingestion metadata
- record hash support
- auditability
- partial recovery support

Operational note:
- The /votacoes endpoint may present HTTP 504 Gateway Timeout during deep pagination.
- This notebook is preserved for controlled API validation and compatibility testing.
- For high-volume operational ingestion, the CSV fallback strategy is recommended.

Recommended operational fallback:
- 04a_bronze_votacoes_csv_fallback

Bronze layer note:
- Technical duplicates, standardization rules and business validation are intentionally handled in the Silver layer.

Source endpoint:
- /votacoes
"""

column_comments = {
    "vot_id_votacao": "Voting session identifier as provided by the Câmara API.",
    "vot_tx_uri": "Voting session URI as provided by the Câmara API.",
    "vot_tx_descricao": "Voting session description as provided by the Câmara API.",
    "vot_dt_data_hora_registro": "Voting registration datetime as provided by the Câmara API.",
    "vot_tx_sigla_orgao": "Legislative body acronym related to the voting session.",
    "vot_tx_aprovacao": "Voting approval information as provided by the Câmara API.",
    "vot_nr_ano_referencia": "Reference year used during API extraction.",
    "vot_tx_payload_json": "Original raw JSON payload preserved from the API response.",
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
    if len(failed_years) > 0
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
        f"Bronze votacoes API ingestion completed "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| failed_years={len(failed_years)} "
        f"| reference_years={REFERENCE_YEARS}"
    ),
    started_at=started_at,
    finished_at=finished_at,
    duration_seconds=duration_seconds,
    records_read=records_read,
    records_written=records_written,
)

if len(failed_years) > 0:

    log_warning(
        pipeline_logger=logger,
        message=(
            f"Bronze votacoes API completed with failed years "
            f"| failed_years={len(failed_years)}"
        ),
    )

else:

    log_success(
        pipeline_logger=logger,
        message=(
            f"Bronze votacoes API ingestion completed "
            f"| duration_seconds={duration_seconds}"
        ),
    )

# COMMAND ----------

print("=" * 90)
print("BRONZE VOTACOES API COMPLETED")
print("=" * 90)
print(f"Target Table: {TARGET_TABLE}")
print(f"Reference Years: {REFERENCE_YEARS}")
print(f"Max Pages: {MAX_PAGES}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Failed Years: {len(failed_years)}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)