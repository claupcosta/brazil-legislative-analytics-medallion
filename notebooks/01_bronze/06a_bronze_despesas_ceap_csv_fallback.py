# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — CEAP Expenses CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `06a_bronze_despesas_ceap_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/Endpoint:** `Official CSV fallback files`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_despesas_ceap`
# MAGIC
# MAGIC Loads CEAP expense records from official CSV fallback files
# MAGIC stored in Unity Catalog Volume and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves raw source fidelity,
# MAGIC including ingestion metadata, original file lineage and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Discover CEAP CSV files dynamically
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
# MAGIC - CEAP extraction scope is controlled through `CEAP_REFERENCE_YEARS`
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
print("06A - BRONZE DESPESAS CEAP CSV FALLBACK")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "06a_bronze_despesas_ceap_csv_fallback"
LAYER_NAME = "bronze"
ENTITY_NAME = "despesas_ceap"

SOURCE_FILE_PATH = VOLUME_RAW_CEAP

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["despesas_ceap"]
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
#
# CSV fallback is the recommended operational ingestion strategy
# for CEAP expense records.
#
# Files are dynamically discovered from Unity Catalog Volume
# and filtered using centralized reference year configuration.
#
# ============================================================

REFERENCE_YEARS = CEAP_CSV_REFERENCE_YEARS

VALID_FILE_YEAR_PATTERN = r"(\d{4})"

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
        VALID_FILE_YEAR_PATTERN,
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
        f"Bronze despesas CEAP CSV fallback ingestion started "
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
        f"Starting CEAP CSV fallback ingestion "
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
            f"No valid CEAP CSV files found for configured reference years "
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
            f"CEAP CSV files discovered dynamically "
            f"| files={len(csv_files)} "
            f"| discovered_years={discovered_years}"
        ),
    )

    if len(ignored_files) > 0:

        log_warning(
            pipeline_logger=logger,
            message=(
                f"Ignored files during CEAP discovery "
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
            f"Failed discovering CEAP CSV files "
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
        message="CEAP CSV source discovery failed.",
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
            f"CEAP CSV records read "
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
            f"Failed reading CEAP CSV files "
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
        message="CEAP CSV reading failed.",
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
        "desp_tx_payload_json",
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
    "txNomeParlamentar": "dep_tx_nome_parlamentar",
    "cpf": "dep_tx_cpf",
    "ideCadastro": "dep_id_deputado",
    "nuCarteiraParlamentar": "dep_nr_carteira_parlamentar",
    "nuLegislatura": "dep_nr_legislatura",
    "sgUF": "dep_tx_sigla_uf",
    "sgPartido": "dep_tx_sigla_partido",
    "codLegislatura": "dep_cd_legislatura",
    "numSubCota": "desp_nr_subcota",
    "txtDescricao": "desp_tx_tipo_despesa",
    "numEspecificacaoSubCota": "desp_nr_especificacao_subcota",
    "txtDescricaoEspecificacao": "desp_tx_descricao_especificacao",
    "txtFornecedor": "desp_tx_nome_fornecedor",
    "txtCNPJCPF": "desp_tx_cnpj_cpf_fornecedor",
    "txtNumero": "desp_tx_numero_documento",
    "indTipoDocumento": "desp_tx_tipo_documento",
    "datEmissao": "desp_dt_data_documento",
    "vlrDocumento": "desp_vl_documento",
    "vlrGlosa": "desp_vl_glosa",
    "vlrLiquido": "desp_vl_liquido",
    "numMes": "desp_nr_mes",
    "numAno": "desp_nr_ano",
    "numParcela": "desp_nr_parcela",
    "txtPassageiro": "desp_tx_passageiro",
    "txtTrecho": "desp_tx_trecho",
    "numLote": "desp_nr_lote",
    "numRessarcimento": "desp_nr_ressarcimento",
    "vlrRestituicao": "desp_vl_restituicao",
    "nuDeputadoId": "dep_id_deputado_original",
    "ideDocumento": "desp_id_documento",
    "urlDocumento": "desp_tx_url_documento",
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
        "desp_nr_ano_referencia",
        regexp_extract(
            col("aud_tx_arquivo_origem"),
            VALID_FILE_YEAR_PATTERN,
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
    "dep_id_deputado",
    "dep_tx_nome_parlamentar",
    "dep_tx_sigla_partido",
    "dep_tx_sigla_uf",
    "dep_nr_legislatura",
    "desp_nr_ano",
    "desp_nr_ano_referencia",
    "desp_nr_mes",
    "desp_tx_tipo_despesa",
    "desp_tx_tipo_documento",
    "desp_tx_numero_documento",
    "desp_tx_nome_fornecedor",
    "desp_tx_cnpj_cpf_fornecedor",
    "desp_dt_data_documento",
    "desp_vl_documento",
    "desp_vl_glosa",
    "desp_vl_liquido",
    "desp_tx_url_documento",
    "desp_tx_payload_json",
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
        "dep_id_deputado",
        "desp_nr_ano",
        "desp_nr_ano_referencia",
        "desp_nr_mes",
        "desp_tx_numero_documento",
        "desp_tx_payload_json",
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
        f"Bronze despesas CEAP CSV fallback table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw CEAP expense ingestion table from official CSV fallback files.

This Bronze table preserves CEAP expense records loaded from CSV files stored in Unity Catalog Volume.

Main characteristics:
- CSV fallback ingestion
- centralized reference year filtering
- raw source payload preservation
- original file lineage
- ingestion metadata
- record hash support
- auditability

Fallback decision:
- The Câmara API endpoint /deputados/{id}/despesas may present timeout behavior.
- CSV fallback was implemented to preserve analytical delivery continuity and improve operational stability.

Reference year note:
- Reference year extraction is based on standardized source file naming conventions.

Bronze layer note:
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source path:
- /Volumes/brazil_legislative_analytics/bronze/raw_files/ceap/
"""

column_comments = {
    "dep_id_deputado": "Deputy identifier associated with the CEAP expense record.",
    "dep_tx_nome_parlamentar": "Parliamentary name as provided by the CSV source.",
    "dep_tx_sigla_partido": "Deputy political party acronym as provided by the CSV source.",
    "dep_tx_sigla_uf": "Deputy Brazilian state acronym as provided by the CSV source.",
    "dep_nr_legislatura": "Legislature number as provided by the CSV source.",
    "desp_nr_ano": "Expense year as provided by the CSV source.",
    "desp_nr_ano_referencia": "Reference year extracted from source file name.",
    "desp_nr_mes": "Expense month as provided by the CSV source.",
    "desp_tx_tipo_despesa": "Expense type description as provided by the CSV source.",
    "desp_tx_tipo_documento": "Expense document type as provided by the CSV source.",
    "desp_tx_numero_documento": "Expense document number as provided by the CSV source.",
    "desp_tx_nome_fornecedor": "Supplier name as provided by the CSV source.",
    "desp_tx_cnpj_cpf_fornecedor": "Supplier CNPJ or CPF as provided by the CSV source.",
    "desp_dt_data_documento": "Expense document date as provided by the CSV source.",
    "desp_vl_documento": "Expense document value as provided by the CSV source.",
    "desp_vl_glosa": "Expense disallowed amount as provided by the CSV source.",
    "desp_vl_liquido": "Expense net amount as provided by the CSV source.",
    "desp_tx_url_documento": "URL of the expense document as provided by the CSV source.",
    "desp_tx_payload_json": "Original raw JSON payload generated from the CSV record.",
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
        f"Bronze despesas CEAP CSV fallback ingestion completed successfully "
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
        f"Bronze despesas CEAP CSV fallback ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE DESPESAS CEAP CSV FALLBACK COMPLETED")
print("=" * 90)
print(f"Source Path: {SOURCE_FILE_PATH}")
print(f"CSV Files: {len(csv_files)}")
print(f"Discovered Years: {discovered_years}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)