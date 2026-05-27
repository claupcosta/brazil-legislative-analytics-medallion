# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Voting Sessions CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `04a_bronze_votacoes_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `Official CSV fallback files`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_votacoes`
# MAGIC
# MAGIC Loads voting session records from official CSV fallback files
# MAGIC stored in Unity Catalog Volume and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw source fidelity,
# MAGIC including ingestion metadata, original file lineage and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Discover voting CSV files dynamically
# MAGIC - Filter source files by configured reference years
# MAGIC - Load official Câmara CSV fallback files
# MAGIC - Standardize Bronze ingestion columns
# MAGIC - Preserve raw source payloads
# MAGIC - Generate ingestion traceability metadata
# MAGIC - Generate deterministic record hashes
# MAGIC - Persist Bronze Delta tables
# MAGIC - Register operational execution logs
# MAGIC - Preserve original source file lineage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves source-system extraction fidelity
# MAGIC - CSV fallback is the recommended operational ingestion strategy
# MAGIC - Voting extraction scope is controlled through `DEFAULT_REFERENCE_YEARS`
# MAGIC - Original file lineage is preserved for auditability
# MAGIC - Reference year extraction is based on standardized file naming conventions
# MAGIC - CSV fallback improves ingestion stability, scalability and execution predictability
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

# MAGIC  %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------


from datetime import datetime
import uuid
import re

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    regexp_extract,
    struct,
    to_json,
)

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("04A - BRONZE VOTACOES CSV FALLBACK")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

NOTEBOOK_NAME = "04a_bronze_votacoes_csv_fallback"
LAYER_NAME = "bronze"
ENTITY_NAME = "votacoes"

SOURCE_FILE_PATH = VOLUME_RAW_VOTACOES

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["votacoes"]
)

LOAD_TYPE = LOAD_TYPE_FALLBACK

execution_id = str(uuid.uuid4())
started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

# COMMAND ----------

REFERENCE_YEARS = VOTACOES_CSV_REFERENCE_YEARS

VALID_FILE_PATTERN = r"votacoes-(\d{4})\.csv"

CSV_SEPARATOR = ";"
CSV_ENCODING = "UTF-8"

OUTPUT_REPARTITION = 8

APPLY_GOVERNANCE_COMMENTS = True

# COMMAND ----------

def apply_table_and_column_comments(
    target_table: str,
    table_comment: str,
    column_comments: dict,
) -> None:
    """
    Applies governance comments to Unity Catalog tables and columns.
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

def extract_reference_year_from_filename(
    file_path: str,
) -> str:
    """
    Extracts reference year from CSV file name.
    """

    matched_year = re.search(
        VALID_FILE_PATTERN,
        file_path,
    )

    if matched_year:
        return matched_year.group(1)

    return None

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
        f"Bronze votacoes CSV fallback ingestion started "
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
        f"Starting votacoes CSV fallback ingestion "
        f"| reference_years={REFERENCE_YEARS}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Discover CSV Files Dynamically

# COMMAND ----------

try:

    source_files = dbutils.fs.ls(SOURCE_FILE_PATH)

    csv_files = []
    ignored_files = []
    discovered_years = []

    for file_info in source_files:

        file_path = file_info.path

        if not file_path.lower().endswith(".csv"):
            ignored_files.append(file_path)
            continue

        extracted_year = extract_reference_year_from_filename(
            file_path=file_path,
        )

        if extracted_year is None:
            ignored_files.append(file_path)
            continue

        extracted_year_int = int(extracted_year)

        if extracted_year_int not in REFERENCE_YEARS:
            ignored_files.append(file_path)
            continue

        csv_files.append(file_path)
        discovered_years.append(extracted_year_int)

    csv_files = sorted(csv_files)
    discovered_years = sorted(list(set(discovered_years)))

    if len(csv_files) == 0:

        raise Exception(
            f"No valid votacoes CSV files found for configured reference years "
            f"| reference_years={REFERENCE_YEARS}"
        )

    display(
        spark.createDataFrame(
            [(str(file_path),) for file_path in csv_files],
            ["csv_file_path"],
        )
    )

    log_info(
        pipeline_logger=logger,
        message=(
            f"Votacoes CSV files discovered dynamically "
            f"| files={len(csv_files)} "
            f"| discovered_years={discovered_years}"
        ),
    )

    if len(ignored_files) > 0:

        log_warning(
            pipeline_logger=logger,
            message=(
                f"Ignored files during votacoes discovery "
                f"| ignored_files={len(ignored_files)}"
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
            f"Failed discovering votacoes CSV files "
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
        message="CSV file discovery failed.",
        error=error,
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
            col("_metadata.file_path"),
        )
    )

    records_read = raw_csv_df.count()

    print("SOURCE CSV COLUMNS")
    print(raw_csv_df.columns)

    log_info(
        pipeline_logger=logger,
        message=(
            f"CSV records loaded successfully "
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
        message=f"CSV ingestion failed | error={str(error)}",
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=None,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="CSV ingestion failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize Bronze Columns

# COMMAND ----------

column_mapping = {
    "id": "vot_id_votacao",
    "idVotacao": "vot_id_votacao",
    "id_votacao": "vot_id_votacao",

    "uri": "vot_tx_uri",
    "uriVotacao": "vot_tx_uri",

    "descricao": "vot_tx_descricao",
    "descricaoVotacao": "vot_tx_descricao",

    "dataHoraRegistro": "vot_dt_data_hora_registro",
    "dataHora": "vot_dt_data_hora_registro",
    "data_hora_registro": "vot_dt_data_hora_registro",

    "siglaOrgao": "vot_tx_sigla_orgao",
    "sigla_orgao": "vot_tx_sigla_orgao",

    "aprovacao": "vot_tx_aprovacao",
    "resultado": "vot_tx_aprovacao",
}

bronze_df = raw_csv_df

for source_column, target_column in column_mapping.items():

    if source_column in bronze_df.columns:

        bronze_df = bronze_df.withColumnRenamed(
            source_column,
            target_column,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add Bronze Metadata Columns

# COMMAND ----------

source_columns_after_mapping = bronze_df.columns

bronze_df = (
    bronze_df
    .withColumn(
        "vot_nr_ano_referencia",
        regexp_extract(
            col("aud_tx_arquivo_origem"),
            r"votacoes-(\d{4})",
            1,
        ),
    )
    .withColumn(
        "vot_tx_payload_json",
        to_json(
            struct(
                *[
                    col(column_name)
                    for column_name in source_columns_after_mapping
                ]
            )
        ),
    )
    .withColumn("aud_id_execucao", lit(execution_id))
    .withColumn("aud_dh_ingestao", current_timestamp())
    .withColumn("aud_tx_endpoint_origem", lit(SOURCE_FILE_PATH))
    .withColumn("aud_tx_sistema_origem", lit("csv_fallback"))
    .withColumn("aud_tx_versao_pipeline", lit(PROJECT_VERSION))
    .withColumn("aud_tx_tipo_carga", lit(LOAD_TYPE))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ensure Expected Bronze Schema

# COMMAND ----------

expected_columns = [
    "vot_id_votacao",
    "vot_tx_uri",
    "vot_tx_descricao",
    "vot_dt_data_hora_registro",
    "vot_tx_sigla_orgao",
    "vot_tx_aprovacao",
    "vot_nr_ano_referencia",
    "vot_tx_payload_json",
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
            lit(None),
        )

bronze_df = bronze_df.select(
    *expected_columns
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Add Deterministic Hash

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
# MAGIC ## 8. Persist Bronze Table

# COMMAND ----------

bronze_df = bronze_df.repartition(
    OUTPUT_REPARTITION
)

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
        f"Bronze votacoes CSV fallback table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Apply Governance Comments

# COMMAND ----------

table_comment = """
Raw voting session records ingested from official CSV fallback files.

This Bronze table stores voting session records extracted from CSV files
stored in Unity Catalog Volume.

Main characteristics:
- CSV fallback ingestion
- voting session traceability
- centralized reference year filtering
- raw source fidelity
- original file lineage
- deterministic hash support
- operational auditability

Fallback decision:
- The /votacoes API endpoint may present HTTP 504 Gateway Timeout during deep pagination.
- CSV fallback improves stability, scalability and execution predictability.

Reference year note:
- Reference year extraction is based on standardized source file naming conventions.

Bronze layer note:
- Technical duplicates, standardization rules and business validation
  are intentionally handled in the Silver layer.
"""

column_comments = {
    "vot_id_votacao": "Voting session identifier.",
    "vot_tx_uri": "Voting session URI.",
    "vot_tx_descricao": "Voting session description.",
    "vot_dt_data_hora_registro": "Voting registration datetime.",
    "vot_tx_sigla_orgao": "Legislative body acronym related to the voting session.",
    "vot_tx_aprovacao": "Voting approval or result information.",
    "vot_nr_ano_referencia": "Reference year extracted from source file name.",
    "vot_tx_payload_json": "Raw JSON payload generated from original CSV record.",
    "aud_id_execucao": "Unique execution identifier for the ingestion run.",
    "aud_dh_ingestao": "Timestamp when the record was ingested into the Bronze layer.",
    "aud_tx_endpoint_origem": "Source volume path used for CSV fallback ingestion.",
    "aud_tx_sistema_origem": "Source system name. For fallback ingestion, this value is csv_fallback.",
    "aud_tx_versao_pipeline": "Pipeline version responsible for the ingestion.",
    "aud_tx_tipo_carga": "Load type applied during ingestion.",
    "aud_tx_arquivo_origem": "Original source file path captured during CSV ingestion.",
    "aud_tx_hash_registro": "Deterministic hash for traceability and deduplication.",
}

if APPLY_GOVERNANCE_COMMENTS:

    apply_table_and_column_comments(
        target_table=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Display Bronze Data Sample

# COMMAND ----------

display(
    bronze_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Final Pipeline Log

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
        f"Bronze votacoes CSV fallback ingestion completed "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| reference_years={REFERENCE_YEARS}"
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
        f"Bronze votacoes CSV fallback ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE VOTACOES CSV FALLBACK COMPLETED")
print("=" * 90)
print(f"Source Path: {SOURCE_FILE_PATH}")
print(f"CSV Files: {len(csv_files)}")
print(f"Discovered Years: {discovered_years}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)