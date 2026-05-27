# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Body Members CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `08a_bronze_orgaos_membros_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `Official CSV fallback files`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_orgaos_membros`
# MAGIC
# MAGIC Loads legislative body membership records from official CSV fallback files
# MAGIC stored in Unity Catalog Volume and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw source fidelity,
# MAGIC including ingestion metadata, original file lineage and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Discover organization member CSV files dynamically
# MAGIC - Filter source files by configured legislatures
# MAGIC - Load official Câmara CSV fallback files
# MAGIC - Standardize Bronze ingestion columns
# MAGIC - Derive organization and deputy identifiers from URIs
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
# MAGIC - Organization member extraction scope is controlled through configured legislature periods
# MAGIC - The `/orgaos/{id}/membros` endpoint may present timeout and instability behavior
# MAGIC - Reference legislature extraction is based on standardized file naming conventions
# MAGIC - CPI classification and analytical specialization are intentionally handled in Silver and Gold layers
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

# MAGIC %run ../99_utils/utils_legislature

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
print("08A - BRONZE ORGAOS MEMBROS CSV FALLBACK")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

NOTEBOOK_NAME = "08a_bronze_orgaos_membros_csv_fallback"
LAYER_NAME = "bronze"
ENTITY_NAME = "orgaos_membros"

SOURCE_FILE_PATH = VOLUME_RAW_ORGAOS_MEMBROS

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["orgaos_membros"]
)

LOAD_TYPE = LOAD_TYPE_FALLBACK

execution_id = str(uuid.uuid4())
started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

# COMMAND ----------

REFERENCE_LEGISLATURES = sorted(
    list(LEGISLATURE_PERIODS.keys())
)

VALID_FILE_PATTERN = r"orgaosDeputados-L(\d+)\.csv"

CSV_SEPARATOR = ";"
CSV_ENCODING = "UTF-8"

OUTPUT_REPARTITION = 4

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

def extract_legislature_from_filename(
    file_path: str,
) -> str:
    """
    Extracts reference legislature from CSV file name.
    """

    matched_legislature = re.search(
        VALID_FILE_PATTERN,
        file_path,
    )

    if matched_legislature:
        return matched_legislature.group(1)

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
        f"Bronze orgaos membros CSV fallback ingestion started "
        f"| reference_legislatures={REFERENCE_LEGISLATURES}"
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
        f"Starting orgaos membros CSV fallback ingestion "
        f"| reference_legislatures={REFERENCE_LEGISLATURES}"
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
    discovered_legislatures = []

    for file_info in source_files:

        file_path = file_info.path

        if not file_path.lower().endswith(".csv"):

            ignored_files.append(file_path)
            continue

        extracted_legislature = extract_legislature_from_filename(
            file_path=file_path,
        )

        if extracted_legislature is None:

            ignored_files.append(file_path)
            continue

        if int(extracted_legislature) not in REFERENCE_LEGISLATURES:

            ignored_files.append(file_path)
            continue

        csv_files.append(file_path)

        discovered_legislatures.append(
            int(extracted_legislature)
        )

    csv_files = sorted(csv_files)

    discovered_legislatures = sorted(
        list(set(discovered_legislatures))
    )

    if len(csv_files) == 0:

        raise Exception(
            f"No valid orgaos membros CSV files found for configured legislatures "
            f"| reference_legislatures={REFERENCE_LEGISLATURES}"
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
            f"Orgaos membros CSV files discovered dynamically "
            f"| files={len(csv_files)} "
            f"| discovered_legislatures={discovered_legislatures}"
        ),
    )

    if len(ignored_files) > 0:

        log_warning(
            pipeline_logger=logger,
            message=(
                f"Ignored files during orgaos membros discovery "
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
            f"Failed discovering orgaos membros CSV files "
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
        message="Orgaos membros CSV source discovery failed.",
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
            f"Orgaos membros CSV records read "
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
            f"Failed reading orgaos membros CSV files "
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
        message="Orgaos membros CSV reading failed.",
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
        "mbr_tx_payload_json",
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
    "uriOrgao": "org_tx_uri",
    "siglaOrgao": "org_tx_sigla",
    "nomeOrgao": "org_tx_nome",
    "nomePublicacaoOrgao": "org_tx_nome_publicacao",
    "uriDeputado": "dep_tx_uri",
    "nomeDeputado": "dep_tx_nome",
    "siglaPartido": "dep_tx_sigla_partido",
    "siglaUF": "dep_tx_sigla_uf",
    "cargo": "mbr_tx_cargo",
    "dataInicio": "mbr_dt_inicio",
    "dataFim": "mbr_dt_fim",
}

for source_column, target_column in column_mapping.items():

    if source_column in bronze_df.columns:

        bronze_df = bronze_df.withColumnRenamed(
            source_column,
            target_column,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Derive Identifiers and Metadata

# COMMAND ----------

bronze_df = (
    bronze_df
    .withColumn(
        "org_id_orgao",
        regexp_extract(
            col("org_tx_uri"),
            r"/orgaos/([0-9]+)",
            1,
        ),
    )
    .withColumn(
        "dep_id_deputado",
        regexp_extract(
            col("dep_tx_uri"),
            r"/deputados/([0-9]+)",
            1,
        ),
    )
    .withColumn(
        "mbr_nr_legislatura_referencia",
        regexp_extract(
            col("aud_tx_arquivo_origem"),
            r"L(\d+)",
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
    "org_id_orgao",
    "org_tx_uri",
    "org_tx_sigla",
    "org_tx_nome",
    "org_tx_nome_publicacao",
    "dep_id_deputado",
    "dep_tx_uri",
    "dep_tx_nome",
    "dep_tx_sigla_partido",
    "dep_tx_sigla_uf",
    "mbr_tx_cargo",
    "mbr_dt_inicio",
    "mbr_dt_fim",
    "mbr_nr_legislatura_referencia",
    "mbr_tx_payload_json",
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
        "org_id_orgao",
        "dep_id_deputado",
        "mbr_nr_legislatura_referencia",
        "mbr_tx_cargo",
        "mbr_dt_inicio",
        "mbr_dt_fim",
        "mbr_tx_payload_json",
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
        f"Bronze orgaos membros CSV fallback table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw legislative body members ingestion table from official CSV fallback files.

This Bronze table preserves organization membership records loaded from CSV files stored in Unity Catalog Volume.

Main characteristics:
- CSV fallback ingestion
- configured legislature filtering
- organization-to-member traceability
- original file lineage
- original payload preservation
- ingestion metadata
- record hash support
- auditability

Fallback decision:
- The Câmara API endpoint /orgaos/{id}/membros may present timeout and instability behavior.
- CSV fallback was implemented to preserve analytical delivery continuity.

Reference legislature note:
- Reference legislature extraction is based on standardized source file naming conventions.

Architecture decision:
- CPI membership classification is not applied in Bronze.
- CPI-related memberships are derived in Silver/Gold from the complete organization membership dataset.

Bronze layer note:
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source path:
- /Volumes/brazil_legislative_analytics/bronze/raw_files/orgaos_membros/
"""

column_comments = {
    "org_id_orgao": "Legislative body identifier derived from the organization URI.",
    "org_tx_uri": "Legislative body URI as provided by the CSV source.",
    "org_tx_sigla": "Legislative body acronym as provided by the CSV source.",
    "org_tx_nome": "Legislative body name as provided by the CSV source.",
    "org_tx_nome_publicacao": "Legislative body publication name as provided by the CSV source.",
    "dep_id_deputado": "Deputy identifier derived from the deputy URI.",
    "dep_tx_uri": "Deputy URI as provided by the CSV source.",
    "dep_tx_nome": "Deputy name as provided by the CSV source.",
    "dep_tx_sigla_partido": "Deputy political party acronym as provided by the CSV source.",
    "dep_tx_sigla_uf": "Deputy Brazilian state acronym as provided by the CSV source.",
    "mbr_tx_cargo": "Member role or position as provided by the CSV source.",
    "mbr_dt_inicio": "Membership start date as provided by the CSV source.",
    "mbr_dt_fim": "Membership end date as provided by the CSV source.",
    "mbr_nr_legislatura_referencia": "Legislature number derived from the source file name.",
    "mbr_tx_payload_json": "Original raw JSON payload generated from the CSV record.",
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
        f"Bronze orgaos membros CSV fallback ingestion completed successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| reference_legislatures={REFERENCE_LEGISLATURES}"
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
        f"Bronze orgaos membros CSV fallback ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE ORGAOS MEMBROS CSV FALLBACK COMPLETED")
print("=" * 90)
print(f"Source Path: {SOURCE_FILE_PATH}")
print(f"CSV Files: {len(csv_files)}")
print(f"Discovered Legislatures: {discovered_legislatures}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)