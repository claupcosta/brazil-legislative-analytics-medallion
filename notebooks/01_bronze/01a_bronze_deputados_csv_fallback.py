# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Bronze Layer — Deputies CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `01a_bronze_deputados_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/File:** `deputados.csv`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_deputados`
# MAGIC
# MAGIC Loads deputy records from the official Câmara dos Deputados CSV fallback source
# MAGIC and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook was designed as a resilient fallback ingestion strategy
# MAGIC for scenarios where the Câmara Open Data API presents instability,
# MAGIC timeout issues or execution limitations in shared environments.
# MAGIC
# MAGIC This notebook preserves raw source extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Load deputy records from official CSV files
# MAGIC - Filter records by supported legislatures
# MAGIC - Preserve raw source payloads
# MAGIC - Generate ingestion traceability metadata
# MAGIC - Generate deterministic record hashes
# MAGIC - Persist Bronze Delta tables
# MAGIC - Register operational execution logs
# MAGIC - Support offline and reproducible ingestion strategy
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves source-system extraction fidelity
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - Supports legislature-scoped extraction strategy
# MAGIC - Governance comments can be optionally applied to tables and columns
# MAGIC - Some API-exclusive attributes may not exist in the CSV source
# MAGIC - Missing API attributes are preserved as NULL in Bronze
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Fallback Strategy
# MAGIC
# MAGIC This notebook replaces direct API ingestion when:
# MAGIC
# MAGIC - the `/deputados` endpoint becomes unstable
# MAGIC - API throttling impacts execution
# MAGIC - shared compute environments present timeout limitations
# MAGIC - reproducible ingestion is required
# MAGIC
# MAGIC The CSV fallback strategy improves:
# MAGIC
# MAGIC - pipeline stability
# MAGIC - execution predictability
# MAGIC - auditability
# MAGIC - reproducibility
# MAGIC - operational resilience
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Source Files
# MAGIC
# MAGIC Expected CSV location:
# MAGIC
# MAGIC `/Volumes/brazil_legislative_analytics/bronze/raw_files/deputados/`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Governance and Architecture
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

# MAGIC %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_legislature

# COMMAND ----------



# ============================================================
# IMPORTS
# ============================================================

from datetime import datetime
import uuid

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    regexp_extract,
    to_json,
    struct,
    when,
)

from pyspark.sql.types import (
    StringType,
    IntegerType,
)

# COMMAND ----------

# ============================================================
# EXECUTION HEADER
# ============================================================

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("01A - BRONZE DEPUTADOS CSV FALLBACK")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "01a_bronze_deputados_csv_fallback"
LAYER_NAME = "bronze"
ENTITY_NAME = "deputados"

SOURCE_FILE_PATH = f"{VOLUME_RAW_FILES}/deputados"

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["deputados"]
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

CSV_SEPARATOR = ";"
CSV_ENCODING = "UTF-8"

REFERENCE_LEGISLATURES = get_supported_legislatures()

APPLY_GOVERNANCE_COMMENTS = True

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
        f"Bronze deputados CSV fallback ingestion started "
        f"| source_path={SOURCE_FILE_PATH}"
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
        f"Starting deputados CSV fallback ingestion "
        f"| source_path={SOURCE_FILE_PATH}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read CSV Files

# COMMAND ----------

try:

    source_files_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", CSV_SEPARATOR)
        .option("encoding", CSV_ENCODING)
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .load(f"{SOURCE_FILE_PATH}/*.csv")
        .withColumn(
            "aud_tx_arquivo_origem",
            col("_metadata.file_path")
        )
    )

    log_info(
        pipeline_logger=logger,
        message="Deputados CSV file loaded successfully.",
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
            f"Failed reading deputados CSV files "
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
        message="Failed reading deputados CSV files.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Normalize Legislature Scope

# COMMAND ----------

reference_legislatures_df = spark.createDataFrame(
    [(int(legislature_id),) for legislature_id in REFERENCE_LEGISLATURES],
    ["dep_id_legislatura_referencia_int"]
)

deputies_with_legislature_df = (
    source_files_df
    .withColumn(
        "dep_id_deputado",
        regexp_extract(
            col("uri"),
            r"/deputados/(\d+)",
            1
        )
    )
    .withColumn(
        "id_legislatura_inicial_int",
        col("idLegislaturaInicial").cast(IntegerType())
    )
    .withColumn(
        "id_legislatura_final_int",
        when(
            col("idLegislaturaFinal").isNull()
            | (col("idLegislaturaFinal") == ""),
            col("idLegislaturaInicial")
        ).otherwise(
            col("idLegislaturaFinal")
        ).cast(IntegerType())
    )
    .join(
        reference_legislatures_df,
        (
            col("dep_id_legislatura_referencia_int")
            >= col("id_legislatura_inicial_int")
        )
        & (
            col("dep_id_legislatura_referencia_int")
            <= col("id_legislatura_final_int")
        ),
        "inner"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Bronze Columns

# COMMAND ----------

bronze_df = (
    deputies_with_legislature_df
    .select(
        col("dep_id_deputado").cast(StringType()).alias("dep_id_deputado"),
        col("uri").cast(StringType()).alias("dep_tx_uri"),
        col("nome").cast(StringType()).alias("dep_tx_nome"),

        lit(None).cast(StringType()).alias("dep_tx_sigla_partido"),
        lit(None).cast(StringType()).alias("dep_tx_uri_partido"),

        col("ufNascimento").cast(StringType()).alias("dep_tx_sigla_uf"),
        col("dep_id_legislatura_referencia_int").cast(StringType()).alias("dep_id_legislatura"),
        col("dep_id_legislatura_referencia_int").cast(StringType()).alias("dep_id_legislatura_referencia"),

        lit(None).cast(StringType()).alias("dep_tx_url_foto"),
        lit(None).cast(StringType()).alias("dep_tx_email"),

        col("nomeCivil").cast(StringType()).alias("dep_tx_nome_civil"),
        col("cpf").cast(StringType()).alias("dep_tx_cpf"),
        col("siglaSexo").cast(StringType()).alias("dep_tx_sigla_sexo"),
        col("urlRedeSocial").cast(StringType()).alias("dep_tx_url_rede_social"),
        col("urlWebsite").cast(StringType()).alias("dep_tx_url_website"),
        col("dataNascimento").cast(StringType()).alias("dep_dt_nascimento"),
        col("dataFalecimento").cast(StringType()).alias("dep_dt_falecimento"),
        col("ufNascimento").cast(StringType()).alias("dep_tx_uf_nascimento"),
        col("municipioNascimento").cast(StringType()).alias("dep_tx_municipio_nascimento"),

        col("aud_tx_arquivo_origem").cast(StringType()).alias("aud_tx_arquivo_origem"),
    )
    .withColumn(
        "dep_tx_payload_json",
        to_json(
            struct("*")
        )
    )
    .withColumn(
        "aud_id_execucao",
        lit(execution_id)
    )
    .withColumn(
        "aud_dh_ingestao",
        current_timestamp()
    )
    .withColumn(
        "aud_tx_endpoint_origem",
        lit("csv_fallback/deputados.csv")
    )
    .withColumn(
        "aud_tx_sistema_origem",
        lit("camara_csv")
    )
    .withColumn(
        "aud_tx_versao_pipeline",
        lit(PROJECT_VERSION)
    )
    .withColumn(
        "aud_tx_tipo_carga",
        lit(LOAD_TYPE)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add Record Hash

# COMMAND ----------

bronze_df = add_hash(
    dataframe=bronze_df,
    columns=[
        "dep_id_deputado",
        "dep_id_legislatura_referencia",
        "dep_tx_payload_json",
    ],
    hash_column="aud_tx_hash_registro",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Persist Bronze Table

# COMMAND ----------

records_read = bronze_df.count()

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
        f"Bronze deputados CSV fallback table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Apply Governance Comments

# COMMAND ----------

table_comment = """
Raw deputy ingestion table from official Câmara dos Deputados CSV fallback files.

This Bronze table preserves deputy records loaded from CSV files when the API endpoint /deputados is unavailable, unstable or slow.

Important note:
The CSV fallback source may not provide all fields available in the API endpoint, such as party acronym, party URI, institutional email and photo URL.
These unavailable attributes are preserved as null in Bronze and can be enriched later in Silver or Gold if additional sources are available.

Source:
- deputados.csv
"""

column_comments = {
    "dep_id_deputado": "Deputy identifier extracted from the source URI.",
    "dep_tx_uri": "Deputy URI from the official CSV file.",
    "dep_tx_nome": "Deputy public name.",
    "dep_tx_sigla_partido": "Deputy party acronym. Not available in the CSV fallback source.",
    "dep_tx_uri_partido": "Deputy party URI. Not available in the CSV fallback source.",
    "dep_tx_sigla_uf": "Deputy state acronym. In this CSV fallback, derived from birth state when mandate state is unavailable.",
    "dep_id_legislatura": "Legislature identifier generated from the configured supported legislature scope.",
    "dep_id_legislatura_referencia": "Reference legislature used for analytical scope.",
    "dep_tx_url_foto": "Deputy photo URL. Not available in the CSV fallback source.",
    "dep_tx_email": "Deputy institutional email. Not available in the CSV fallback source.",
    "dep_tx_nome_civil": "Deputy civil name.",
    "dep_tx_cpf": "Deputy CPF when available in source file.",
    "dep_tx_sigla_sexo": "Deputy gender acronym as provided by the source file.",
    "dep_tx_url_rede_social": "Deputy social network URLs.",
    "dep_tx_url_website": "Deputy website URL.",
    "dep_dt_nascimento": "Deputy birth date.",
    "dep_dt_falecimento": "Deputy death date, when applicable.",
    "dep_tx_uf_nascimento": "Deputy birth state acronym.",
    "dep_tx_municipio_nascimento": "Deputy birth municipality.",
    "dep_tx_payload_json": "Original raw payload preserved as JSON.",
    "aud_tx_arquivo_origem": "Source CSV file path.",
    "aud_id_execucao": "Unique execution identifier.",
    "aud_dh_ingestao": "Bronze ingestion timestamp.",
    "aud_tx_endpoint_origem": "Logical source endpoint or CSV fallback source.",
    "aud_tx_sistema_origem": "Source system name.",
    "aud_tx_versao_pipeline": "Pipeline version responsible for the ingestion.",
    "aud_tx_tipo_carga": "Load type applied during ingestion.",
    "aud_tx_hash_registro": "Deterministic hash used for traceability and deduplication.",
}

if APPLY_GOVERNANCE_COMMENTS:

    escaped_table_comment = table_comment.replace("'", "''")

    spark.sql(f"""
    COMMENT ON TABLE {TARGET_TABLE}
    IS '{escaped_table_comment}'
    """)

    for column_name, column_comment in column_comments.items():

        escaped_column_comment = column_comment.replace("'", "''")

        spark.sql(f"""
        ALTER TABLE {TARGET_TABLE}
        ALTER COLUMN {column_name}
        COMMENT '{escaped_column_comment}'
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Final Pipeline Log

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
        f"Bronze deputados CSV fallback ingestion completed successfully "
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
        f"Bronze deputados CSV fallback ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE DEPUTADOS CSV FALLBACK COMPLETED")
print("=" * 90)
print(f"Source Path: {SOURCE_FILE_PATH}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Reference Legislatures: {REFERENCE_LEGISLATURES}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)