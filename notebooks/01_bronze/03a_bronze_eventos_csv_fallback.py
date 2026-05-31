# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Bronze Layer — Legislative Events CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `03a_bronze_eventos_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/File:** `eventos-YYYY.csv`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_eventos`
# MAGIC
# MAGIC Loads legislative event records from official Câmara dos Deputados CSV fallback files
# MAGIC and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves the same core Bronze schema used by the original API ingestion notebook,
# MAGIC ensuring compatibility with downstream Silver notebooks.
# MAGIC
# MAGIC This notebook also preserves additional CSV attributes to improve analytical coverage
# MAGIC while keeping raw source fidelity, ingestion metadata and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read legislative event CSV fallback files
# MAGIC - Preserve event identification, schedule, status and location information
# MAGIC - Maintain compatibility with the original Bronze API schema
# MAGIC - Preserve additional CSV attributes available in the official source file
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
# MAGIC - CSV fallback replaces direct API ingestion when `/eventos` is unstable
# MAGIC - Silver compatibility is preserved through the original `evt_*` field naming standard
# MAGIC - Additional CSV attributes are persisted as Bronze columns and preserved in the raw payload
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

# MAGIC  %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Layer — Legislative Events CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `03a_bronze_eventos_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/File:** `eventos-YYYY.csv`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_eventos`
# MAGIC
# MAGIC Loads legislative event records from official Câmara dos Deputados CSV fallback files
# MAGIC and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook preserves the same core Bronze schema used by the original API ingestion notebook,
# MAGIC ensuring compatibility with downstream Silver notebooks.
# MAGIC
# MAGIC This notebook also preserves additional CSV attributes to improve analytical coverage
# MAGIC while keeping raw source fidelity, ingestion metadata and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read legislative event CSV fallback files
# MAGIC - Preserve event identification, schedule, status and location information
# MAGIC - Maintain compatibility with the original Bronze API schema
# MAGIC - Preserve additional CSV attributes available in the official source file
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
# MAGIC - CSV fallback replaces direct API ingestion when `/eventos` is unstable
# MAGIC - Silver compatibility is preserved through the original `evt_*` field naming standard
# MAGIC - Additional CSV attributes are persisted as Bronze columns and preserved in the raw payload
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
    concat_ws,
)

from pyspark.sql.types import StringType

# COMMAND ----------

# ============================================================
# EXECUTION HEADER
# ============================================================

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("03A - BRONZE EVENTOS CSV FALLBACK")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "03a_bronze_eventos_csv_fallback"
LAYER_NAME = "bronze"
ENTITY_NAME = "eventos"

SOURCE_FILE_PATH = f"{VOLUME_RAW_FILES}/eventos"

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["eventos"]
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
    Handles CSV column names containing dots by escaping them with backticks.
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
        f"Bronze eventos CSV fallback ingestion started "
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
        f"Starting eventos CSV fallback ingestion "
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
        message="Eventos CSV files loaded successfully.",
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
            f"Failed reading eventos CSV files "
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
        message="Failed reading eventos CSV files.",
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
        "evt_tx_payload_json",
        to_json(struct("*"))
    )
)

bronze_df = (
    source_with_payload_df
    .select(
        get_source_column(source_with_payload_df, "id")
            .cast(StringType())
            .alias("evt_id_evento"),

        get_source_column(source_with_payload_df, "descricao")
            .cast(StringType())
            .alias("evt_tx_descricao"),

        concat_ws(
            " - ",
            get_source_column(source_with_payload_df, "localExterno"),
            get_source_column(source_with_payload_df, "localCamara.nome"),
            get_source_column(source_with_payload_df, "localCamara.predio"),
            get_source_column(source_with_payload_df, "localCamara.sala"),
            get_source_column(source_with_payload_df, "localCamara.andar"),
        )
            .cast(StringType())
            .alias("evt_tx_local"),

        get_source_column(source_with_payload_df, "dataHoraInicio")
            .cast(StringType())
            .alias("evt_dt_data_hora_inicio"),

        get_source_column(source_with_payload_df, "dataHoraFim")
            .cast(StringType())
            .alias("evt_dt_data_hora_fim"),

        get_source_column(source_with_payload_df, "situacao")
            .cast(StringType())
            .alias("evt_tx_situacao"),

        get_source_column(source_with_payload_df, "uri")
            .cast(StringType())
            .alias("evt_tx_uri"),

        regexp_extract(
            col("aud_tx_arquivo_origem"),
            r"eventos-(\d{4})\.csv",
            1
        )
            .cast(StringType())
            .alias("evt_nr_ano_referencia"),

        regexp_extract(
            col("aud_tx_arquivo_origem"),
            r"eventos-(\d{4})\.csv",
            1
        )
            .cast(StringType())
            .alias("evt_nr_ano_arquivo"),

        concat_ws(
            "-",
            regexp_extract(
                col("aud_tx_arquivo_origem"),
                r"eventos-(\d{4})\.csv",
                1
            ),
            lit("01"),
            lit("01")
        )
            .cast(StringType())
            .alias("evt_dt_inicio_janela"),

        concat_ws(
            "-",
            regexp_extract(
                col("aud_tx_arquivo_origem"),
                r"eventos-(\d{4})\.csv",
                1
            ),
            lit("12"),
            lit("31")
        )
            .cast(StringType())
            .alias("evt_dt_fim_janela"),

        get_source_column(source_with_payload_df, "descricaoTipo")
            .cast(StringType())
            .alias("evt_tx_tipo_evento"),

        get_source_column(source_with_payload_df, "urlDocumentoPauta")
            .cast(StringType())
            .alias("evt_tx_url_documento_pauta"),

        get_source_column(source_with_payload_df, "localExterno")
            .cast(StringType())
            .alias("evt_tx_local_externo"),

        get_source_column(source_with_payload_df, "localCamara.nome")
            .cast(StringType())
            .alias("evt_tx_local_camara_nome"),

        get_source_column(source_with_payload_df, "localCamara.predio")
            .cast(StringType())
            .alias("evt_tx_local_camara_predio"),

        get_source_column(source_with_payload_df, "localCamara.sala")
            .cast(StringType())
            .alias("evt_tx_local_camara_sala"),

        get_source_column(source_with_payload_df, "localCamara.andar")
            .cast(StringType())
            .alias("evt_tx_local_camara_andar"),

        col("evt_tx_payload_json")
            .cast(StringType())
            .alias("evt_tx_payload_json"),

        col("aud_tx_arquivo_origem")
            .cast(StringType())
            .alias("aud_tx_arquivo_origem"),
    )
    .withColumn("aud_id_execucao", lit(execution_id))
    .withColumn("aud_dh_ingestao", current_timestamp())
    .withColumn("aud_tx_endpoint_origem", lit("csv_fallback/eventos-YYYY.csv"))
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
        "evt_dt_data_hora_inicio",
        "evt_tx_payload_json",
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
        f"Bronze eventos CSV fallback table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw legislative event ingestion table from official Câmara dos Deputados CSV fallback files.

This Bronze table preserves legislative event records loaded from eventos-YYYY.csv when the API endpoint /eventos is unavailable, unstable or slow.

Main characteristics:
- raw ingestion fidelity
- event identification preservation
- schedule and status preservation
- location information preservation
- original payload preservation
- ingestion metadata
- record hash support
- auditability
- CSV fallback resilience
- compatibility with the original API Bronze schema
- additional CSV attributes preserved for downstream enrichment

Architecture note:
- The original API-compatible fields are preserved using the evt_* naming standard.
- Additional CSV attributes are also persisted as Bronze fields.
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source:
- csv_fallback/eventos-YYYY.csv
"""

column_comments = {
    "evt_id_evento": "Legislative event identifier as provided by the official Câmara source.",
    "evt_tx_descricao": "Legislative event description as provided by the official Câmara source.",
    "evt_tx_local": "Legislative event location composed from external and Câmara location attributes.",
    "evt_dt_data_hora_inicio": "Event start datetime as provided by the official Câmara source.",
    "evt_dt_data_hora_fim": "Event end datetime as provided by the official Câmara source.",
    "evt_tx_situacao": "Event status as provided by the official Câmara source.",
    "evt_tx_uri": "Legislative event URI as provided by the official Câmara source.",
    "evt_nr_ano_referencia": "Reference year extracted from the source CSV file name.",
    "evt_nr_ano_arquivo": "Source file year extracted from the source CSV file name.",
    "evt_dt_inicio_janela": "Extraction window start date derived from the source file reference year.",
    "evt_dt_fim_janela": "Extraction window end date derived from the source file reference year.",
    "evt_tx_tipo_evento": "Legislative event type description as provided by the CSV source.",
    "evt_tx_url_documento_pauta": "URL for the event agenda document as provided by the CSV source.",
    "evt_tx_local_externo": "External event location as provided by the CSV source.",
    "evt_tx_local_camara_nome": "Câmara internal location name as provided by the CSV source.",
    "evt_tx_local_camara_predio": "Câmara building location as provided by the CSV source.",
    "evt_tx_local_camara_sala": "Câmara room location as provided by the CSV source.",
    "evt_tx_local_camara_andar": "Câmara floor location as provided by the CSV source.",
    "evt_tx_payload_json": "Original raw payload preserved from the CSV source as JSON.",
    "aud_tx_arquivo_origem": "Source CSV file path.",
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
        f"Bronze eventos CSV fallback ingestion completed successfully "
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
        f"Bronze eventos CSV fallback ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE EVENTOS CSV FALLBACK COMPLETED")
print("=" * 90)
print(f"Source Path: {SOURCE_FILE_PATH}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)