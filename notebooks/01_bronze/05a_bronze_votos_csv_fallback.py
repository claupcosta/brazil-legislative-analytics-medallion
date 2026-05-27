# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Individual Votes CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `05a_bronze_votos_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `Official CSV fallback files`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_votos`
# MAGIC
# MAGIC Loads individual voting records from official CSV fallback files
# MAGIC stored in Unity Catalog Volume and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw source fidelity,
# MAGIC including ingestion metadata, original file lineage and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Discover individual voting CSV files dynamically
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
# MAGIC - Individual vote extraction scope is controlled through `DEFAULT_REFERENCE_YEARS`
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

# MAGIC  %run ../99_utils/utils_legislature

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
print("05A - BRONZE VOTOS CSV FALLBACK")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

NOTEBOOK_NAME = "05a_bronze_votos_csv_fallback"
LAYER_NAME = "bronze"
ENTITY_NAME = "votos"

SOURCE_FILE_PATH = VOLUME_RAW_VOTOS

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["votos"]
)

LOAD_TYPE = LOAD_TYPE_FALLBACK

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

REFERENCE_YEARS = VOTOS_CSV_REFERENCE_YEARS

VALID_FILE_PATTERN = r"votacoesVotos-(\d{4})\.csv"

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
        f"Bronze votos CSV fallback ingestion started "
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
        f"Starting votos CSV fallback ingestion "
        f"| reference_years={REFERENCE_YEARS}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Discover CSV Files Dynamically

# COMMAND ----------

try:

    source_files = dbutils.fs.ls(
        SOURCE_FILE_PATH
    )

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
            f"No valid votos CSV files found for configured reference years "
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
            f"Votos CSV files discovered dynamically "
            f"| files={len(csv_files)} "
            f"| discovered_years={discovered_years}"
        ),
    )

    if len(ignored_files) > 0:

        log_warning(
            pipeline_logger=logger,
            message=(
                f"Ignored files during votos discovery "
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
            f"Failed discovering votos CSV files "
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
        message=(
            f"CSV ingestion failed "
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
        message="CSV ingestion failed.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize Bronze Columns

# COMMAND ----------

column_mapping = {
    "idVotacao": "vot_id_votacao",
    "id_votacao": "vot_id_votacao",
    "votacao_id": "vot_id_votacao",
    "uriVotacao": "vot_tx_uri_votacao",

    "idDeputado": "dep_id_deputado",
    "id_deputado": "dep_id_deputado",
    "deputado_id": "dep_id_deputado",
    "ideCadastro": "dep_id_deputado",

    "nomeDeputado": "dep_tx_nome",
    "nome": "dep_tx_nome",
    "deputado_nome": "dep_tx_nome",

    "siglaPartido": "dep_tx_sigla_partido",
    "sigla_partido": "dep_tx_sigla_partido",
    "partido": "dep_tx_sigla_partido",

    "siglaUf": "dep_tx_sigla_uf",
    "siglaUF": "dep_tx_sigla_uf",
    "sigla_uf": "dep_tx_sigla_uf",
    "uf": "dep_tx_sigla_uf",

    "tipoVoto": "vot_tx_tipo_voto",
    "voto": "vot_tx_tipo_voto",
    "orientacao": "vot_tx_tipo_voto",

    "dataRegistroVoto": "vot_dt_registro_voto",
    "dataHoraVoto": "vot_dt_registro_voto",
    "data_hora_voto": "vot_dt_registro_voto",
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
            r"votacoesVotos-(\d{4})",
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
    "vot_nr_ano_referencia",
    "dep_id_deputado",
    "dep_tx_nome",
    "dep_tx_sigla_partido",
    "dep_tx_sigla_uf",
    "vot_tx_tipo_voto",
    "vot_dt_registro_voto",
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
        "dep_id_deputado",
        "vot_tx_tipo_voto",
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
        f"Bronze votos CSV fallback table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Apply Governance Comments

# COMMAND ----------

table_comment = """
Raw individual voting records ingested from official CSV fallback files.

This Bronze table stores deputy-level voting records extracted from
CSV files stored in Unity Catalog Volume.

Main characteristics:
- high-volume ingestion strategy
- CSV fallback ingestion
- centralized reference year filtering
- voting traceability
- deputy vote metadata
- raw source fidelity
- original file lineage
- deterministic hash support
- operational auditability

Fallback decision:
- The /votacoes/{id}/votos API endpoint requires one request per voting session.
- CSV fallback improves stability, scalability and execution predictability.

Reference year note:
- Reference year extraction is based on standardized source file naming conventions.

Bronze layer note:
- Technical duplicates, standardization rules and business validation
  are intentionally handled in the Silver layer.
"""

column_comments = {
    "vot_id_votacao": "Voting session identifier.",
    "vot_nr_ano_referencia": "Reference year extracted from source file name.",
    "dep_id_deputado": "Deputy identifier.",
    "dep_tx_nome": "Deputy name.",
    "dep_tx_sigla_partido": "Deputy political party acronym.",
    "dep_tx_sigla_uf": "Deputy state acronym.",
    "vot_tx_tipo_voto": "Deputy vote value.",
    "vot_dt_registro_voto": "Vote registration datetime.",
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
        f"Bronze votos CSV fallback ingestion completed "
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
        f"Bronze votos CSV fallback ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE VOTOS CSV FALLBACK COMPLETED")
print("=" * 90)
print(f"Source Path: {SOURCE_FILE_PATH}")
print(f"CSV Files: {len(csv_files)}")
print(f"Discovered Years: {discovered_years}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)