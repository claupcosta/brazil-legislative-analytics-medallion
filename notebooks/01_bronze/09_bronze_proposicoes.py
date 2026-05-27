# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Propositions API Ingestion
# MAGIC
# MAGIC **Notebook:** `09_bronze_proposicoes`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `/proposicoes`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_proposicoes`
# MAGIC
# MAGIC Extracts legislative proposition records from the Câmara dos Deputados
# MAGIC Open Data API and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw API extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Extract legislative proposition records from the Câmara API
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
# MAGIC - Propositions are extracted by reference year to improve reliability and operational control
# MAGIC - Extraction scope is controlled through `PROPOSICOES_REFERENCE_YEARS`
# MAGIC - The `/proposicoes` endpoint may present timeout and pagination instability behavior
# MAGIC - CSV fallback ingestion is recommended for high-volume operational processing
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - Governance comments are applied to tables and columns
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

# MAGIC  %run ../99_utils/utils_pagination

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_hash

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
print("09 - BRONZE PROPOSICOES")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "09_bronze_proposicoes"
LAYER_NAME = "bronze"
ENTITY_NAME = "proposicoes"

SOURCE_ENDPOINT = API_ENDPOINTS["proposicoes"]

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["proposicoes"]
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
# Propositions are extracted by reference year to reduce API volume,
# improve reliability and support partial recovery.
#
# Reference years are centralized in 00_setup/01_project_config.
#
# ============================================================

PAGE_SIZE = 25
MAX_PAGES = None

REFERENCE_YEARS = PROPOSICOES_REFERENCE_YEARS

REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRY_ATTEMPTS = 3
SLEEP_BETWEEN_YEARS_SECONDS = 0.5

BASE_PARAMS = {
    "ordem": "ASC",
    "ordenarPor": "id",
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
        f"Bronze proposicoes ingestion started "
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
        f"Starting proposicoes ingestion "
        f"| reference_years={REFERENCE_YEARS}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract Propositions by Year

# COMMAND ----------

try:

    proposition_records = []
    failed_years = []

    for reference_year in REFERENCE_YEARS:

        year_params = dict(BASE_PARAMS)
        year_params["ano"] = reference_year

        log_info(
            pipeline_logger=logger,
            message=(
                f"Starting proposicoes extraction "
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

            proposition_records.extend(
                year_records
            )

            log_info(
                pipeline_logger=logger,
                message=(
                    f"Proposicoes extracted for year "
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
                    f"Proposicoes extraction failed for year "
                    f"| reference_year={reference_year} "
                    f"| error={str(year_error)}"
                ),
            )

        if SLEEP_BETWEEN_YEARS_SECONDS > 0:
            time.sleep(SLEEP_BETWEEN_YEARS_SECONDS)

    records_read = len(proposition_records)

    log_info(
        pipeline_logger=logger,
        message=(
            f"Proposition records extracted "
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
            f"Failed during proposicoes extraction "
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
        message="Proposicoes extraction failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Bronze Records

# COMMAND ----------

bronze_rows = []
ingestion_timestamp = datetime.now()

for proposition_record in proposition_records:

    raw_json_payload = json.dumps(
        proposition_record,
        ensure_ascii=False,
    )

    bronze_rows.append({
        "prop_id_proposicao": str(
            proposition_record.get("id")
        ),
        "prop_tx_uri": proposition_record.get(
            "uri"
        ),
        "prop_tx_sigla_tipo": proposition_record.get(
            "siglaTipo"
        ),
        "prop_tx_cod_tipo": str(
            proposition_record.get("codTipo")
        ),
        "prop_tx_numero": str(
            proposition_record.get("numero")
        ),
        "prop_nr_ano": str(
            proposition_record.get("ano")
        ),
        "prop_tx_ementa": proposition_record.get(
            "ementa"
        ),
        "prop_tx_descricao_tipo": proposition_record.get(
            "descricaoTipo"
        ),
        "prop_tx_ementa_detalhada": proposition_record.get(
            "ementaDetalhada"
        ),
        "prop_tx_keywords": proposition_record.get(
            "keywords"
        ),
        "prop_dt_apresentacao": str(
            proposition_record.get("dataApresentacao")
        ),
        "prop_tx_uri_orgao_numerador": proposition_record.get(
            "uriOrgaoNumerador"
        ),
        "prop_tx_status_uri": proposition_record.get(
            "uriPropPrincipal"
        ),
        "prop_nr_ano_referencia": str(
            proposition_record.get("anoReferencia")
        ),
        "prop_tx_payload_json": raw_json_payload,
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
    StructField("prop_id_proposicao", StringType(), True),
    StructField("prop_tx_uri", StringType(), True),
    StructField("prop_tx_sigla_tipo", StringType(), True),
    StructField("prop_tx_cod_tipo", StringType(), True),
    StructField("prop_tx_numero", StringType(), True),
    StructField("prop_nr_ano", StringType(), True),
    StructField("prop_tx_ementa", StringType(), True),
    StructField("prop_tx_descricao_tipo", StringType(), True),
    StructField("prop_tx_ementa_detalhada", StringType(), True),
    StructField("prop_tx_keywords", StringType(), True),
    StructField("prop_dt_apresentacao", StringType(), True),
    StructField("prop_tx_uri_orgao_numerador", StringType(), True),
    StructField("prop_tx_status_uri", StringType(), True),
    StructField("prop_nr_ano_referencia", StringType(), True),
    StructField("prop_tx_payload_json", StringType(), True),
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
        "prop_id_proposicao",
        "prop_nr_ano_referencia",
        "prop_tx_payload_json",
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
        f"Bronze proposicoes table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Apply Governance Comments

# COMMAND ----------

table_comment = """
Raw legislative propositions ingestion table from Câmara dos Deputados Open Data API.

This Bronze table preserves legislative proposition records extracted from the /proposicoes endpoint.

Main characteristics:
- raw ingestion fidelity
- year-based extraction strategy
- centralized reference year configuration
- original payload preservation
- ingestion metadata
- record hash support
- auditability
- partial recovery by reference year

Operational note:
- The /proposicoes endpoint may present timeout or pagination instability.
- CSV fallback ingestion is recommended for high-volume operational processing.

Bronze layer note:
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source endpoint:
- /proposicoes
"""

column_comments = {
    "prop_id_proposicao": "Legislative proposition identifier as provided by the Câmara API.",
    "prop_tx_uri": "Legislative proposition URI as provided by the Câmara API.",
    "prop_tx_sigla_tipo": "Proposition type acronym as provided by the Câmara API.",
    "prop_tx_cod_tipo": "Proposition type code as provided by the Câmara API.",
    "prop_tx_numero": "Proposition number as provided by the Câmara API.",
    "prop_nr_ano": "Proposition year as provided by the Câmara API.",
    "prop_tx_ementa": "Proposition summary text as provided by the Câmara API.",
    "prop_tx_descricao_tipo": "Proposition type description as provided by the Câmara API.",
    "prop_tx_ementa_detalhada": "Detailed proposition summary as provided by the Câmara API.",
    "prop_tx_keywords": "Keywords associated with the proposition as provided by the Câmara API.",
    "prop_dt_apresentacao": "Proposition presentation date as provided by the Câmara API.",
    "prop_tx_uri_orgao_numerador": "URI of the legislative body responsible for proposition numbering.",
    "prop_tx_status_uri": "URI of the main proposition when available.",
    "prop_nr_ano_referencia": "Reference year used to extract the proposition record.",
    "prop_tx_payload_json": "Original raw JSON payload preserved from the API response.",
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
        f"Bronze proposicoes ingestion completed "
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
            f"Bronze proposicoes completed with failed years "
            f"| failed_years={len(failed_years)}"
        ),
    )

else:

    log_success(
        pipeline_logger=logger,
        message=(
            f"Bronze proposicoes ingestion completed "
            f"| duration_seconds={duration_seconds}"
        ),
    )

# COMMAND ----------

print("=" * 90)
print("BRONZE PROPOSICOES COMPLETED")
print("=" * 90)
print(f"Target Table: {TARGET_TABLE}")
print(f"Reference Years: {REFERENCE_YEARS}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Failed Years: {len(failed_years)}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)