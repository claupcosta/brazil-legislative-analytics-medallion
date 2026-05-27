# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Individual Votes API Ingestion
# MAGIC
# MAGIC **Notebook:** `05_bronze_votos`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `/votacoes/{id}/votos`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_votos`
# MAGIC
# MAGIC Extracts individual vote records from voting sessions using the
# MAGIC Câmara dos Deputados Open Data API and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw API extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Load voting session identifiers from Bronze voting tables
# MAGIC - Filter voting sessions by reference years when available
# MAGIC - Extract individual vote records from the Câmara API
# MAGIC - Preserve raw API payloads
# MAGIC - Generate ingestion traceability metadata
# MAGIC - Generate deterministic record hashes
# MAGIC - Persist Bronze Delta tables
# MAGIC - Register operational execution logs
# MAGIC - Track failed voting identifiers without interrupting the full ingestion
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves source-system extraction fidelity
# MAGIC - Voting session scope is controlled through `DEFAULT_REFERENCE_YEARS` when available from Bronze voting data
# MAGIC - The `/votacoes/{id}/votos` endpoint is consumed without pagination parameters
# MAGIC - Some voting identifiers may return empty responses or API errors without interrupting the full ingestion
# MAGIC - API ingestion is recommended mainly for validation and controlled extraction scenarios
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

from pyspark.sql.functions import col

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("05 - BRONZE VOTOS API")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "05_bronze_votos"
LAYER_NAME = "bronze"
ENTITY_NAME = "votos"

SOURCE_VOTACOES_TABLE = get_bronze_table(
    BRONZE_TABLES["votacoes"]
)

SOURCE_ENDPOINT_TEMPLATE = API_ENDPOINTS["votos"]

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["votos"]
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
# Individual votes depend on voting sessions previously ingested
# by 04_bronze_votacoes.
#
# The /votacoes/{id}/votos endpoint must be consumed without
# pagination parameters.
#
# API ingestion is intentionally controlled because this endpoint
# may generate many requests and some voting sessions may return
# empty responses, HTTP 400 or HTTP 404.
#
# For high-volume operational ingestion, use the CSV fallback
# notebook: 05a_bronze_votos_csv_fallback.
#
# ============================================================

REFERENCE_YEARS = DEFAULT_REFERENCE_YEARS

MAX_VOTACOES = 50

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRY_ATTEMPTS = 2
SLEEP_BETWEEN_REQUESTS_SECONDS = 0.2

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

def request_votes_without_pagination(
    endpoint_path: str,
) -> list:
    """
    Requests individual vote records without pagination parameters.

    The /votacoes/{id}/votos endpoint may return HTTP 400 when called
    with pagina/itens parameters. Therefore, this request is executed
    directly using requests, without relying on pagination utilities.
    """

    import requests

    last_error = None

    for attempt_number in range(1, MAX_RETRY_ATTEMPTS + 1):

        try:

            url = f"{CAMARA_API_BASE_URL}{endpoint_path}"

            response = requests.get(
                url=url,
                timeout=REQUEST_TIMEOUT_SECONDS,
                headers={
                    "accept": "application/json",
                },
            )

            response.raise_for_status()

            response_payload = response.json()

            if response_payload is None:
                return []

            return response_payload.get(
                "dados",
                [],
            )

        except Exception as error:

            last_error = error

            if attempt_number < MAX_RETRY_ATTEMPTS:
                time.sleep(SLEEP_BETWEEN_REQUESTS_SECONDS)

    raise Exception(
        f"Vote request failed after {MAX_RETRY_ATTEMPTS} attempt(s) "
        f"| endpoint={endpoint_path} "
        f"| last_error={str(last_error)}"
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
        f"Bronze votos API ingestion started "
        f"| reference_years={REFERENCE_YEARS} "
        f"| max_votacoes={MAX_VOTACOES}"
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
        f"Starting votos API ingestion "
        f"| reference_years={REFERENCE_YEARS} "
        f"| max_votacoes={MAX_VOTACOES}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Voting Identifiers

# COMMAND ----------

try:

    source_votacoes_df = spark.table(SOURCE_VOTACOES_TABLE)

    voting_ids_df = (
        source_votacoes_df
        .select(
            *[
                column_name
                for column_name in [
                    "vot_id_votacao",
                    "vot_nr_ano_referencia",
                ]
                if column_name in source_votacoes_df.columns
            ]
        )
        .where("vot_id_votacao IS NOT NULL")
        .dropDuplicates()
    )

    if "vot_nr_ano_referencia" in voting_ids_df.columns:

        reference_year_values = [
            str(reference_year)
            for reference_year in REFERENCE_YEARS
        ]

        voting_ids_df = (
            voting_ids_df
            .where(
                col("vot_nr_ano_referencia").isin(
                    reference_year_values
                )
            )
        )

    voting_ids_df = voting_ids_df.orderBy("vot_id_votacao")

    if MAX_VOTACOES is not None:
        voting_ids_df = voting_ids_df.limit(MAX_VOTACOES)

    voting_rows = voting_ids_df.collect()

    log_info(
        pipeline_logger=logger,
        message=(
            f"Voting identifiers loaded "
            f"| total_votacoes={len(voting_rows)} "
            f"| reference_years={REFERENCE_YEARS}"
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
            f"Failed loading voting identifiers "
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
        message="Failed loading voting identifiers.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extract Vote Records

# COMMAND ----------

try:

    vote_records = []
    failed_voting_ids = []

    total_votacoes = len(voting_rows)

    for index, voting_row in enumerate(
        voting_rows,
        start=1,
    ):

        voting_id = voting_row["vot_id_votacao"]

        voting_reference_year = (
            voting_row["vot_nr_ano_referencia"]
            if "vot_nr_ano_referencia" in voting_row.asDict()
            else None
        )

        endpoint_path = SOURCE_ENDPOINT_TEMPLATE.replace(
            "{id}",
            str(voting_id),
        )

        log_info(
            pipeline_logger=logger,
            message=(
                f"Extracting votes "
                f"| voting_id={voting_id} "
                f"| reference_year={voting_reference_year} "
                f"| position={index}/{total_votacoes}"
            ),
        )

        try:

            voting_vote_records = request_votes_without_pagination(
                endpoint_path=endpoint_path,
            )

            for record in voting_vote_records:
                record["idVotacao"] = voting_id
                record["anoReferenciaVotacao"] = voting_reference_year

            vote_records.extend(
                voting_vote_records
            )

            log_info(
                pipeline_logger=logger,
                message=(
                    f"Votes extracted "
                    f"| voting_id={voting_id} "
                    f"| reference_year={voting_reference_year} "
                    f"| records={len(voting_vote_records)}"
                ),
            )

        except Exception as vote_error:

            failed_voting_ids.append({
                "voting_id": voting_id,
                "reference_year": voting_reference_year,
                "error": str(vote_error),
            })

            log_warning(
                pipeline_logger=logger,
                message=(
                    f"Vote extraction failed "
                    f"| voting_id={voting_id} "
                    f"| reference_year={voting_reference_year} "
                    f"| error={str(vote_error)}"
                ),
            )

        if SLEEP_BETWEEN_REQUESTS_SECONDS > 0:
            time.sleep(SLEEP_BETWEEN_REQUESTS_SECONDS)

    records_read = len(vote_records)

    log_info(
        pipeline_logger=logger,
        message=(
            f"Vote records extracted "
            f"| records_read={records_read} "
            f"| failed_voting_ids={len(failed_voting_ids)}"
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
            f"Failed during votos extraction "
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
        message="Votos extraction failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Bronze Records

# COMMAND ----------

bronze_rows = []
ingestion_timestamp = datetime.now()

for vote_record in vote_records:

    raw_json_payload = json.dumps(
        vote_record,
        ensure_ascii=False,
    )

    deputy_data = (
        vote_record.get("deputado_")
        or vote_record.get("deputado")
        or {}
    )

    bronze_rows.append({
        "vot_id_votacao": str(
            vote_record.get("idVotacao")
        ),
        "vot_nr_ano_referencia": str(
            vote_record.get("anoReferenciaVotacao")
        ),
        "dep_id_deputado": str(
            deputy_data.get("id")
        ),
        "dep_tx_nome": deputy_data.get(
            "nome"
        ),
        "dep_tx_sigla_partido": deputy_data.get(
            "siglaPartido"
        ),
        "dep_tx_sigla_uf": deputy_data.get(
            "siglaUf"
        ),
        "vot_tx_tipo_voto": vote_record.get(
            "tipoVoto"
        ),
        "vot_dt_registro_voto": str(
            vote_record.get("dataRegistroVoto")
        ),
        "vot_tx_payload_json": raw_json_payload,
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
    StructField("vot_id_votacao", StringType(), True),
    StructField("vot_nr_ano_referencia", StringType(), True),
    StructField("dep_id_deputado", StringType(), True),
    StructField("dep_tx_nome", StringType(), True),
    StructField("dep_tx_sigla_partido", StringType(), True),
    StructField("dep_tx_sigla_uf", StringType(), True),
    StructField("vot_tx_tipo_voto", StringType(), True),
    StructField("vot_dt_registro_voto", StringType(), True),
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
# MAGIC ## 6. Add Record Hash

# COMMAND ----------

bronze_df = add_hash(
    dataframe=bronze_df,
    columns=[
        "vot_id_votacao",
        "vot_nr_ano_referencia",
        "dep_id_deputado",
        "vot_tx_tipo_voto",
        "vot_tx_payload_json",
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
        f"Bronze votos API table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw individual vote ingestion table from Câmara dos Deputados Open Data API.

This Bronze table preserves individual vote records extracted from the /votacoes/{id}/votos endpoint.

Main characteristics:
- raw ingestion fidelity
- vote-to-voting-session traceability
- reference year inherited from voting session ingestion
- deputy vote metadata
- original payload preservation
- ingestion metadata
- record hash support
- auditability

Operational note:
- The /votacoes/{id}/votos endpoint is consumed without pagination parameters.
- Some voting identifiers may return empty responses, HTTP 400 or HTTP 404 depending on API availability and voting session type.
- These cases are logged as warnings and do not stop the full ingestion.
- API ingestion is recommended mainly for validation and controlled extraction scenarios.
- For high-volume ingestion, the CSV fallback strategy is recommended.

Recommended operational fallback:
- 05a_bronze_votos_csv_fallback

Bronze layer note:
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source endpoint:
- /votacoes/{id}/votos
"""

column_comments = {
    "vot_id_votacao": "Voting session identifier associated with the individual vote record.",
    "vot_nr_ano_referencia": "Reference year inherited from the Bronze voting session record.",
    "dep_id_deputado": "Deputy identifier associated with the vote record.",
    "dep_tx_nome": "Deputy name as provided by the API.",
    "dep_tx_sigla_partido": "Deputy political party acronym as provided by the API.",
    "dep_tx_sigla_uf": "Deputy Brazilian state acronym as provided by the API.",
    "vot_tx_tipo_voto": "Individual vote value as provided by the API.",
    "vot_dt_registro_voto": "Vote registration datetime as provided by the API.",
    "vot_tx_payload_json": "Original raw JSON payload preserved from the API response.",
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
    if len(failed_voting_ids) > 0
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
        f"Bronze votos API ingestion completed "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| failed_voting_ids={len(failed_voting_ids)} "
        f"| reference_years={REFERENCE_YEARS}"
    ),
    started_at=started_at,
    finished_at=finished_at,
    duration_seconds=duration_seconds,
    records_read=records_read,
    records_written=records_written,
)

if len(failed_voting_ids) > 0:

    log_warning(
        pipeline_logger=logger,
        message=(
            f"Bronze votos API completed with failed voting identifiers "
            f"| failed_voting_ids={len(failed_voting_ids)}"
        ),
    )

else:

    log_success(
        pipeline_logger=logger,
        message=(
            f"Bronze votos API ingestion completed "
            f"| duration_seconds={duration_seconds}"
        ),
    )

# COMMAND ----------

print("=" * 90)
print("BRONZE VOTOS API COMPLETED")
print("=" * 90)
print(f"Target Table: {TARGET_TABLE}")
print(f"Reference Years: {REFERENCE_YEARS}")
print(f"Voting Sessions Processed: {len(voting_rows)}")
print(f"Max Voting Sessions: {MAX_VOTACOES}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Failed Voting IDs: {len(failed_voting_ids)}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)