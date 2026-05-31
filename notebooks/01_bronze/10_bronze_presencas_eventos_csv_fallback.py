# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Bronze Layer — Event Attendance CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `11_bronze_presencas_eventos_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/File:** `eventosPresencaDeputados-YYYY.csv`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_presencas_eventos`
# MAGIC
# MAGIC Loads deputy event attendance records from official Câmara dos Deputados CSV fallback files
# MAGIC and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook creates the Bronze relationship table between legislative events and deputies,
# MAGIC supporting the analytical construction of the Parliamentary Attendance and Absenteeism Monitor.
# MAGIC
# MAGIC This notebook preserves raw source extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read event attendance CSV fallback files
# MAGIC - Preserve the relationship between legislative events and deputies
# MAGIC - Preserve event date and deputy attendance context
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
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - CSV fallback supports execution stability and reproducibility
# MAGIC - This table supports the mandatory Attendance and Absenteeism Monitor deliverable
# MAGIC - Original source payloads are preserved for auditability
# MAGIC - Governance comments are applied to tables and columns
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/decisions/api_limitations.md`
# MAGIC - `/docs/governance/data_lineage.md`
# MAGIC - `/docs/governance/data_quality_rules.md`
# MAGIC

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

# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Layer — Event Attendance CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `11_bronze_presencas_eventos_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/File:** `eventosPresencaDeputados-YYYY.csv`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_presencas_eventos`
# MAGIC
# MAGIC Loads deputy event attendance records from official Câmara dos Deputados CSV fallback files
# MAGIC and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook creates the Bronze relationship table between legislative events and deputies,
# MAGIC supporting the analytical construction of the Parliamentary Attendance and Absenteeism Monitor.
# MAGIC
# MAGIC This notebook preserves raw source extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read event attendance CSV fallback files
# MAGIC - Preserve the relationship between legislative events and deputies
# MAGIC - Preserve event date and deputy attendance context
# MAGIC - Generate a presence flag for attendance records
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
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - CSV fallback supports execution stability and reproducibility
# MAGIC - This table supports the mandatory Attendance and Absenteeism Monitor deliverable
# MAGIC - Each source row represents a confirmed deputy attendance event
# MAGIC - Original source payloads are preserved for auditability
# MAGIC - Governance comments are applied to tables and columns
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/decisions/api_limitations.md`
# MAGIC - `/docs/governance/data_lineage.md`
# MAGIC - `/docs/governance/data_quality_rules.md`

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

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
    to_json,
    struct,
    regexp_extract,
)

from pyspark.sql.types import StringType

# COMMAND ----------

# ============================================================
# EXECUTION HEADER
# ============================================================

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("11 - BRONZE PRESENCAS EVENTOS CSV FALLBACK")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "11_bronze_presencas_eventos_csv_fallback"
LAYER_NAME = "bronze"
ENTITY_NAME = "presencas_eventos"

SOURCE_FILE_PATH = f"{VOLUME_RAW_FILES}/presenca_eventos"

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["presencas_eventos"]
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

APPLY_GOVERNANCE_COMMENTS = True

# COMMAND ----------

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_source_column(dataframe, column_name: str):
    """
    Returns a source column when it exists.
    Otherwise returns a null literal to keep the target schema stable.
    """

    if column_name in dataframe.columns:
        return col(f"`{column_name}`")

    return lit(None)


def apply_table_and_column_comments(
    target_table: str,
    table_comment: str,
    column_comments: dict,
) -> None:
    """
    Applies table and column comments for Unity Catalog governance documentation.
    """

    escaped_table_comment = table_comment.replace("'", "''")

    spark.sql(f"""
    COMMENT ON TABLE {target_table}
    IS '{escaped_table_comment}'
    """)

    for column_name, column_comment in column_comments.items():

        escaped_column_comment = column_comment.replace("'", "''")

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
        f"Bronze presencas eventos CSV fallback ingestion started "
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
        f"Starting presencas eventos CSV fallback ingestion "
        f"| source_path={SOURCE_FILE_PATH}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read CSV Files

# COMMAND ----------

try:

    source_df = (
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
        message="Presencas eventos CSV files loaded successfully.",
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
            f"Failed reading presencas eventos CSV files "
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
        message="Failed reading presencas eventos CSV files.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Bronze Columns

# COMMAND ----------

source_with_payload_df = (
    source_df
    .withColumn(
        "pev_tx_payload_json",
        to_json(struct("*"))
    )
)

bronze_df = (
    source_with_payload_df
    .select(
        get_source_column(source_with_payload_df, "idEvento")
            .cast(StringType())
            .alias("evt_id_evento"),

        get_source_column(source_with_payload_df, "uriEvento")
            .cast(StringType())
            .alias("evt_tx_uri"),

        get_source_column(source_with_payload_df, "dataHoraInicio")
            .cast(StringType())
            .alias("evt_dh_inicio"),

        get_source_column(source_with_payload_df, "idDeputado")
            .cast(StringType())
            .alias("dep_id_deputado"),

        get_source_column(source_with_payload_df, "uriDeputado")
            .cast(StringType())
            .alias("dep_tx_uri"),

        regexp_extract(
            col("aud_tx_arquivo_origem"),
            r"eventosPresencaDeputados-(\d{4})\.csv",
            1
        )
            .cast(StringType())
            .alias("pev_nr_ano_arquivo"),

        col("pev_tx_payload_json")
            .cast(StringType())
            .alias("pev_tx_payload_json"),

        col("aud_tx_arquivo_origem")
            .cast(StringType())
            .alias("aud_tx_arquivo_origem"),
    )
    .withColumn(
        "pev_fl_presenca",
        lit(1)
    )
    .withColumn("aud_id_execucao", lit(execution_id))
    .withColumn("aud_dh_ingestao", current_timestamp())
    .withColumn("aud_tx_endpoint_origem", lit("csv_fallback/eventosPresencaDeputados-YYYY.csv"))
    .withColumn("aud_tx_sistema_origem", lit("camara_csv"))
    .withColumn("aud_tx_versao_pipeline", lit(PROJECT_VERSION))
    .withColumn("aud_tx_tipo_carga", lit(LOAD_TYPE))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Add Record Hash

# COMMAND ----------

bronze_df = add_hash(
    dataframe=bronze_df,
    columns=[
        "evt_id_evento",
        "dep_id_deputado",
        "evt_dh_inicio",
        "pev_fl_presenca",
        "pev_tx_payload_json",
    ],
    hash_column="aud_tx_hash_registro",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persist Bronze Table

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
        f"Bronze presencas eventos CSV fallback table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw event attendance ingestion table from official Câmara dos Deputados CSV fallback files.

This Bronze table preserves the relationship between legislative events and deputies
loaded from eventosPresencaDeputados-YYYY.csv files.

Main characteristics:
- raw ingestion fidelity
- event-to-deputy attendance relationship preservation
- event timestamp preservation
- confirmed presence flag generation
- original payload preservation
- ingestion metadata
- record hash support
- auditability
- CSV fallback resilience

Architecture note:
- Each source row represents a confirmed deputy attendance event.
- This table supports the mandatory Attendance and Absenteeism Monitor deliverable.
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source:
- csv_fallback/eventosPresencaDeputados-YYYY.csv
"""

column_comments = {
    "evt_id_evento": "Legislative event identifier associated with deputy attendance.",
    "evt_tx_uri": "Legislative event URI as provided by the official Câmara source.",
    "evt_dh_inicio": "Legislative event start datetime as provided by the source file.",
    "dep_id_deputado": "Deputy identifier associated with the event attendance record.",
    "dep_tx_uri": "Deputy URI as provided by the official Câmara source.",
    "pev_nr_ano_arquivo": "Reference year extracted from the source CSV file name.",
    "pev_tx_payload_json": "Original raw payload preserved from the CSV source as JSON.",
    "aud_tx_arquivo_origem": "Source CSV file path.",
    "pev_fl_presenca": "Presence flag derived from the attendance source file. Value 1 indicates confirmed attendance.",
    "aud_id_execucao": "Unique execution identifier for the ingestion run.",
    "aud_dh_ingestao": "Timestamp when the record was ingested into the Bronze layer.",
    "aud_tx_endpoint_origem": "Logical source endpoint or CSV fallback source used to extract the record.",
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
# MAGIC ## 7. Final Pipeline Log

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
        f"Bronze presencas eventos CSV fallback ingestion completed successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written}"
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
        f"Bronze presencas eventos CSV fallback ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE PRESENCAS EVENTOS CSV FALLBACK COMPLETED")
print("=" * 90)
print(f"Source Path: {SOURCE_FILE_PATH}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)