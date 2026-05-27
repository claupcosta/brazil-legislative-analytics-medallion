# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Body Members API Ingestion
# MAGIC
# MAGIC **Notebook:** `08_bronze_orgaos_membros`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `/orgaos/{id}/membros`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_orgaos_membros`
# MAGIC
# MAGIC Extracts legislative body membership records from the Câmara dos Deputados
# MAGIC Open Data API and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw API extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Load legislative body identifiers from Bronze organization tables
# MAGIC - Extract legislative body membership records from the Câmara API
# MAGIC - Handle controlled paginated API ingestion
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
# MAGIC - The `/orgaos/{id}/membros` endpoint may present timeout and instability behavior depending on API availability
# MAGIC - This notebook is recommended mainly for validation, controlled extraction and replay scenarios
# MAGIC - CSV fallback ingestion is recommended for operational resilience and large-scale historical ingestion
# MAGIC - Current operational scope prioritizes Legislatures 56 and 57
# MAGIC - CPI classification and analytical specialization are intentionally handled in Silver and Gold layers
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - Governance comments are applied to tables and columns
# MAGIC - Failed organization requests are logged as warnings without interrupting the entire ingestion process
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
print("08 - BRONZE ORGAOS MEMBROS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "08_bronze_orgaos_membros"
LAYER_NAME = "bronze"
ENTITY_NAME = "orgaos_membros"

SOURCE_ORGAOS_TABLE = get_bronze_table(
    BRONZE_TABLES["orgaos"]
)

SOURCE_ENDPOINT_TEMPLATE = API_ENDPOINTS["orgaos_membros"]

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["orgaos_membros"]
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
# Development mode:
# - Keep MAX_ORGAOS limited while validating.
# - Set MAX_ORGAOS = None for full extraction.
#
# ============================================================

PAGE_SIZE = 100
MAX_PAGES = None

MAX_ORGAOS = 50

REFERENCE_START_DATE = "2023-01-01"
REFERENCE_END_DATE = "2026-12-31"

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRY_ATTEMPTS = 2
SLEEP_BETWEEN_REQUESTS_SECONDS = 0.2

BASE_PARAMS = {
    "dataInicio": REFERENCE_START_DATE,
    "dataFim": REFERENCE_END_DATE,
}

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
    message="Bronze orgaos membros ingestion started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting orgaos membros ingestion.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Legislative Body Identifiers

# COMMAND ----------

try:

    orgaos_df = (
        spark.table(SOURCE_ORGAOS_TABLE)
        .select(
            "org_id_orgao",
            "org_tx_sigla",
            "org_tx_nome",
        )
        .where("org_id_orgao IS NOT NULL")
        .dropDuplicates()
        .orderBy("org_id_orgao")
    )

    if MAX_ORGAOS is not None:
        orgaos_df = orgaos_df.limit(MAX_ORGAOS)

    orgao_rows = orgaos_df.collect()

    log_info(
        pipeline_logger=logger,
        message=(
            f"Legislative bodies loaded "
            f"| total_orgaos={len(orgao_rows)}"
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
            f"Failed loading legislative body identifiers "
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
        message="Failed loading legislative body identifiers.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extract Legislative Body Members

# COMMAND ----------

try:

    member_records = []
    failed_orgaos = []

    total_orgaos = len(orgao_rows)

    for index, orgao_row in enumerate(
        orgao_rows,
        start=1,
    ):

        orgao_id = orgao_row["org_id_orgao"]
        orgao_sigla = orgao_row["org_tx_sigla"]
        orgao_nome = orgao_row["org_tx_nome"]

        endpoint_path = SOURCE_ENDPOINT_TEMPLATE.replace(
            "{id}",
            str(orgao_id),
        )

        log_info(
            pipeline_logger=logger,
            message=(
                f"Starting organization members extraction "
                f"| orgao_id={orgao_id} "
                f"| position={index}/{total_orgaos}"
            ),
        )

        try:

            orgao_member_records = collect_pages(
                endpoint_path=endpoint_path,
                base_params=BASE_PARAMS,
                page_size=PAGE_SIZE,
                max_pages=MAX_PAGES,
                request_timeout=REQUEST_TIMEOUT_SECONDS,
                max_retries=MAX_RETRY_ATTEMPTS,
                sleep_seconds=0.5,
            )

            for record in orgao_member_records:
                record["idOrgao"] = orgao_id
                record["siglaOrgao"] = orgao_sigla
                record["nomeOrgao"] = orgao_nome
                record["dataInicioReferencia"] = REFERENCE_START_DATE
                record["dataFimReferencia"] = REFERENCE_END_DATE

            member_records.extend(
                orgao_member_records
            )

            log_info(
                pipeline_logger=logger,
                message=(
                    f"Organization members extracted "
                    f"| orgao_id={orgao_id} "
                    f"| records={len(orgao_member_records)}"
                ),
            )

        except Exception as member_error:

            failed_orgaos.append({
                "orgao_id": orgao_id,
                "error": str(member_error),
            })

            log_warning(
                pipeline_logger=logger,
                message=(
                    f"Organization members extraction failed "
                    f"| orgao_id={orgao_id} "
                    f"| error={str(member_error)}"
                ),
            )

        if SLEEP_BETWEEN_REQUESTS_SECONDS > 0:

            time.sleep(
                SLEEP_BETWEEN_REQUESTS_SECONDS
            )

    records_read = len(member_records)

    log_info(
        pipeline_logger=logger,
        message=(
            f"Organization member records extracted "
            f"| records_read={records_read} "
            f"| failed_orgaos={len(failed_orgaos)}"
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
            f"Failed during organization members extraction "
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
        message="Orgaos membros extraction failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Bronze Records

# COMMAND ----------

bronze_rows = []
ingestion_timestamp = datetime.now()

for member_record in member_records:

    raw_json_payload = json.dumps(
        member_record,
        ensure_ascii=False,
    )

    deputy_data = (
        member_record.get("deputado")
        or member_record.get("deputado_")
        or member_record
    )

    bronze_rows.append({
        "org_id_orgao": str(
            member_record.get("idOrgao")
        ),
        "org_tx_sigla": member_record.get(
            "siglaOrgao"
        ),
        "org_tx_nome": member_record.get(
            "nomeOrgao"
        ),
        "dep_id_deputado": str(
            deputy_data.get("id")
        ),
        "dep_tx_nome": (
            deputy_data.get("nome")
            or member_record.get("nome")
        ),
        "dep_tx_sigla_partido": (
            deputy_data.get("siglaPartido")
            or member_record.get("siglaPartido")
        ),
        "dep_tx_sigla_uf": (
            deputy_data.get("siglaUf")
            or member_record.get("siglaUf")
        ),
        "mbr_tx_cargo": (
            member_record.get("cargo")
            or member_record.get("titulo")
        ),
        "mbr_tx_condicao": member_record.get(
            "condicao"
        ),
        "mbr_dt_inicio": str(
            member_record.get("dataInicio")
        ),
        "mbr_dt_fim": str(
            member_record.get("dataFim")
        ),
        "mbr_dt_inicio_referencia": member_record.get(
            "dataInicioReferencia"
        ),
        "mbr_dt_fim_referencia": member_record.get(
            "dataFimReferencia"
        ),
        "mbr_tx_payload_json": raw_json_payload,
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
    StructField("org_id_orgao", StringType(), True),
    StructField("org_tx_sigla", StringType(), True),
    StructField("org_tx_nome", StringType(), True),
    StructField("dep_id_deputado", StringType(), True),
    StructField("dep_tx_nome", StringType(), True),
    StructField("dep_tx_sigla_partido", StringType(), True),
    StructField("dep_tx_sigla_uf", StringType(), True),
    StructField("mbr_tx_cargo", StringType(), True),
    StructField("mbr_tx_condicao", StringType(), True),
    StructField("mbr_dt_inicio", StringType(), True),
    StructField("mbr_dt_fim", StringType(), True),
    StructField("mbr_dt_inicio_referencia", StringType(), True),
    StructField("mbr_dt_fim_referencia", StringType(), True),
    StructField("mbr_tx_payload_json", StringType(), True),
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
        "org_id_orgao",
        "dep_id_deputado",
        "mbr_tx_payload_json",
    ],
    hash_column="aud_tx_hash_registro",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Persist Bronze Table

# COMMAND ----------

bronze_df.write.format(
    "delta"
).mode(
    "overwrite"
).option(
    "overwriteSchema",
    "true"
).saveAsTable(
    TARGET_TABLE
)

records_written = bronze_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze orgaos membros table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw legislative body members ingestion table from Câmara dos Deputados Open Data API.

This Bronze table preserves membership records extracted from /orgaos/{id}/membros for legislative bodies.

Main characteristics:
- raw ingestion fidelity
- organization-to-member traceability
- temporal membership reference window
- original payload preservation
- ingestion metadata
- record hash support
- auditability

Architecture decision:
- CPI membership classification is not applied in Bronze.
- CPI-related memberships are derived in Silver/Gold from the complete organization membership dataset.

Bronze layer note:
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source endpoint:
- /orgaos/{id}/membros
"""

column_comments = {
    "org_id_orgao": "Legislative body identifier associated with the member record.",
    "org_tx_sigla": "Legislative body acronym associated with the member record.",
    "org_tx_nome": "Legislative body name associated with the member record.",
    "dep_id_deputado": "Deputy identifier associated with the organization member record.",
    "dep_tx_nome": "Deputy name associated with the organization member record.",
    "dep_tx_sigla_partido": "Deputy political party acronym.",
    "dep_tx_sigla_uf": "Deputy Brazilian state acronym.",
    "mbr_tx_cargo": "Member role or position as provided by the API.",
    "mbr_tx_condicao": "Member condition or status as provided by the API.",
    "mbr_dt_inicio": "Membership start date as provided by the API.",
    "mbr_dt_fim": "Membership end date as provided by the API.",
    "mbr_dt_inicio_referencia": "Reference start date used for membership extraction.",
    "mbr_dt_fim_referencia": "Reference end date used for membership extraction.",
    "mbr_tx_payload_json": "Original raw JSON payload preserved from the API response.",
    "aud_id_execucao": "Unique execution identifier for the ingestion run.",
    "aud_dh_ingestao": "Timestamp when the record was ingested into the Bronze layer.",
    "aud_tx_endpoint_origem": "Source API endpoint template used to extract the record.",
    "aud_tx_sistema_origem": "Source system name.",
    "aud_tx_versao_pipeline": "Pipeline version responsible for the ingestion.",
    "aud_tx_tipo_carga": "Load type applied during ingestion.",
    "aud_tx_hash_registro": "Deterministic hash used for traceability and deduplication.",
}

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
    if len(failed_orgaos) > 0
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
        f"Bronze orgaos membros ingestion completed "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| failed_orgaos={len(failed_orgaos)}"
    ),
    started_at=started_at,
    finished_at=finished_at,
    duration_seconds=duration_seconds,
    records_read=records_read,
    records_written=records_written,
)

if len(failed_orgaos) > 0:

    log_warning(
        pipeline_logger=logger,
        message=(
            f"Bronze orgaos membros completed with failed organizations "
            f"| failed_orgaos={len(failed_orgaos)}"
        ),
    )

else:

    log_success(
        pipeline_logger=logger,
        message=(
            f"Bronze orgaos membros ingestion completed "
            f"| duration_seconds={duration_seconds}"
        ),
    )

# COMMAND ----------

print("=" * 90)
print("BRONZE ORGAOS MEMBROS COMPLETED")
print("=" * 90)
print(f"Target Table: {TARGET_TABLE}")
print(f"Organizations Processed: {len(orgao_rows)}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Failed Organizations: {len(failed_orgaos)}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)