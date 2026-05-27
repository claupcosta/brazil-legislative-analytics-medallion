# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Propositions CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `09a_bronze_proposicoes_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `Official CSV fallback files`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_proposicoes`
# MAGIC
# MAGIC Loads legislative proposition records from official CSV fallback files
# MAGIC stored in Unity Catalog Volume and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw source fidelity,
# MAGIC including ingestion metadata, original file lineage and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Discover proposition CSV files dynamically
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
# MAGIC - Proposition extraction scope is controlled through `PROPOSICOES_REFERENCE_YEARS`
# MAGIC - The `/proposicoes` endpoint may present timeout and pagination instability behavior
# MAGIC - Original file lineage is preserved for auditability
# MAGIC - Reference year extraction is based on standardized file naming conventions
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
    lit,
    current_timestamp,
    col,
    to_json,
    struct,
    regexp_extract,
)

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("09A - BRONZE PROPOSICOES CSV FALLBACK")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

NOTEBOOK_NAME = "09a_bronze_proposicoes_csv_fallback"
LAYER_NAME = "bronze"
ENTITY_NAME = "proposicoes"

SOURCE_FILE_PATH = VOLUME_RAW_PROPOSICOES

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["proposicoes"]
)

LOAD_TYPE = LOAD_TYPE_FALLBACK

execution_id = str(uuid.uuid4())
started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

# COMMAND ----------

REFERENCE_YEARS = PROPOSICOES_CSV_REFERENCE_YEARS

VALID_FILE_PATTERN = r"proposicoes-(\d{4})\.csv"

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
        f"Bronze proposicoes CSV fallback ingestion started "
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
        f"Starting proposicoes CSV fallback ingestion "
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

        if int(extracted_year) not in REFERENCE_YEARS:

            ignored_files.append(file_path)
            continue

        csv_files.append(file_path)

        discovered_years.append(
            int(extracted_year)
        )

    csv_files = sorted(csv_files)

    discovered_years = sorted(
        list(set(discovered_years))
    )

    if len(csv_files) == 0:

        raise Exception(
            f"No valid proposicoes CSV files found for configured reference years "
            f"| reference_years={REFERENCE_YEARS}"
        )

    display(
        spark.createDataFrame(
            [(file_path,) for file_path in csv_files],
            ["csv_file_path"],
        )
    )

    log_info(
        pipeline_logger=logger,
        message=(
            f"Proposicoes CSV files discovered dynamically "
            f"| files={len(csv_files)} "
            f"| discovered_years={discovered_years}"
        ),
    )

    if len(ignored_files) > 0:

        log_warning(
            pipeline_logger=logger,
            message=(
                f"Ignored files during proposicoes discovery "
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
            f"Failed discovering proposicoes CSV files "
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
        message="Proposicoes CSV source discovery failed.",
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
            f"Proposicoes CSV records read "
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
            f"Failed reading proposicoes CSV files "
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
        message="Proposicoes CSV reading failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Raw Payload

# COMMAND ----------

source_columns = raw_csv_df.columns

bronze_df = (
    raw_csv_df
    .withColumn(
        "prop_tx_payload_json",
        to_json(
            struct(
                *[
                    col(column_name)
                    for column_name in source_columns
                ]
            )
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Standardize Bronze Columns

# COMMAND ----------

column_mapping = {
    "id": "prop_id_proposicao",
    "uri": "prop_tx_uri",
    "siglaTipo": "prop_tx_sigla_tipo",
    "codTipo": "prop_cd_tipo",
    "numero": "prop_nr_numero",
    "ano": "prop_nr_ano",
    "ementa": "prop_tx_ementa",
    "descricaoTipo": "prop_tx_descricao_tipo",
    "ementaDetalhada": "prop_tx_ementa_detalhada",
    "keywords": "prop_tx_keywords",
    "dataApresentacao": "prop_dt_apresentacao",
    "uriOrgaoNumerador": "prop_tx_uri_orgao_numerador",
    "uriPropPrincipal": "prop_tx_uri_prop_principal",
}

for source_column, target_column in column_mapping.items():

    if source_column in bronze_df.columns:

        bronze_df = bronze_df.withColumnRenamed(
            source_column,
            target_column,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Add Bronze Metadata Columns

# COMMAND ----------

bronze_df = (
    bronze_df
    .withColumn(
        "prop_nr_ano_referencia",
        regexp_extract(
            col("aud_tx_arquivo_origem"),
            r"proposicoes-(\d{4})",
            1,
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
# MAGIC ## 7. Ensure Expected Columns

# COMMAND ----------

expected_columns = [
    "prop_id_proposicao",
    "prop_tx_uri",
    "prop_tx_sigla_tipo",
    "prop_cd_tipo",
    "prop_nr_numero",
    "prop_nr_ano",
    "prop_tx_ementa",
    "prop_tx_descricao_tipo",
    "prop_tx_ementa_detalhada",
    "prop_tx_keywords",
    "prop_dt_apresentacao",
    "prop_tx_uri_orgao_numerador",
    "prop_tx_uri_prop_principal",
    "prop_nr_ano_referencia",
    "prop_tx_payload_json",
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
# MAGIC ## 8. Add Record Hash

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
# MAGIC ## 9. Persist Bronze Table

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
        f"Bronze proposicoes CSV fallback table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw legislative propositions ingestion table from official CSV fallback files.

This Bronze table preserves proposition records loaded from CSV files stored in Unity Catalog Volume.

Main characteristics:
- CSV fallback ingestion
- configured reference year filtering
- historical proposition ingestion
- original file lineage
- original payload preservation
- ingestion metadata
- record hash support
- auditability

Fallback decision:
- The Câmara API endpoint /proposicoes may present timeout, pagination instability or excessive execution time.
- CSV fallback was implemented to preserve analytical delivery continuity and improve operational stability.

Reference year note:
- Reference year extraction is based on standardized source file naming conventions.

Bronze layer note:
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source path:
- /Volumes/brazil_legislative_analytics/bronze/raw_files/proposicoes/
"""

column_comments = {
    "prop_id_proposicao": "Legislative proposition identifier as provided by the CSV source.",
    "prop_tx_uri": "Legislative proposition URI as provided by the CSV source.",
    "prop_tx_sigla_tipo": "Proposition type acronym as provided by the CSV source.",
    "prop_cd_tipo": "Proposition type code as provided by the CSV source.",
    "prop_nr_numero": "Proposition number as provided by the CSV source.",
    "prop_nr_ano": "Proposition year as provided by the CSV source.",
    "prop_tx_ementa": "Proposition summary text as provided by the CSV source.",
    "prop_tx_descricao_tipo": "Proposition type description as provided by the CSV source.",
    "prop_tx_ementa_detalhada": "Detailed proposition summary as provided by the CSV source.",
    "prop_tx_keywords": "Keywords associated with the proposition as provided by the CSV source.",
    "prop_dt_apresentacao": "Proposition presentation date as provided by the CSV source.",
    "prop_tx_uri_orgao_numerador": "URI of the legislative body responsible for proposition numbering.",
    "prop_tx_uri_prop_principal": "URI of the main proposition when available.",
    "prop_nr_ano_referencia": "Reference year derived from the source file name.",
    "prop_tx_payload_json": "Original raw JSON payload generated from the CSV record.",
    "aud_id_execucao": "Unique execution identifier for the ingestion run.",
    "aud_dh_ingestao": "Timestamp when the record was ingested into the Bronze layer.",
    "aud_tx_endpoint_origem": "Source volume path used to extract the CSV records.",
    "aud_tx_sistema_origem": "Source system name. For fallback ingestion, this value is csv_fallback.",
    "aud_tx_versao_pipeline": "Pipeline version responsible for the ingestion.",
    "aud_tx_tipo_carga": "Load type applied during ingestion.",
    "aud_tx_arquivo_origem": "Original source file path captured during CSV ingestion.",
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
# MAGIC ## 11. Display Bronze Data

# COMMAND ----------

display(
    bronze_df.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Final Pipeline Log

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
        f"Bronze proposicoes CSV fallback ingestion completed successfully "
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
        f"Bronze proposicoes CSV fallback ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE PROPOSICOES CSV FALLBACK COMPLETED")
print("=" * 90)
print(f"Source Path: {SOURCE_FILE_PATH}")
print(f"CSV Files: {len(csv_files)}")
print(f"Discovered Years: {discovered_years}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)