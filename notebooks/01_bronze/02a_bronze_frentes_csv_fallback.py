# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Bronze Layer — Parliamentary Fronts CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `02a_bronze_frentes_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/File:** `frentes.csv`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_frentes`
# MAGIC
# MAGIC Extracts parliamentary front records from official Câmara dos Deputados CSV fallback files
# MAGIC and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook is part of the contingency ingestion strategy adopted for API instability,
# MAGIC timeouts or endpoint unavailability scenarios.
# MAGIC
# MAGIC The ingestion preserves raw source fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read parliamentary front CSV fallback files
# MAGIC - Preserve raw source-system extraction fidelity
# MAGIC - Standardize Bronze column naming conventions
# MAGIC - Generate ingestion traceability metadata
# MAGIC - Generate deterministic record hashes
# MAGIC - Persist Bronze Delta tables
# MAGIC - Register operational execution logs
# MAGIC - Support API fallback ingestion strategy
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - This notebook replaces direct API ingestion when the `/frentes` endpoint is unstable
# MAGIC - Bronze preserves source-system extraction fidelity
# MAGIC - Technical duplicates and business validations are handled in Silver
# MAGIC - Original source payloads are preserved for auditability
# MAGIC - Governance comments are applied to tables and columns
# MAGIC - CSV fallback strategy is formally documented as an architectural decision
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
    to_json,
    struct,
)

from pyspark.sql.types import StringType

# COMMAND ----------

# ============================================================
# EXECUTION HEADER
# ============================================================

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("02A - BRONZE FRENTES CSV FALLBACK")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "02a_bronze_frentes_csv_fallback"
LAYER_NAME = "bronze"
ENTITY_NAME = "frentes"

SOURCE_FILE_PATH = f"{VOLUME_RAW_FILES}/frentes"

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["frentes"]
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
        return col(column_name)

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
        f"Bronze frentes CSV fallback ingestion started "
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
        f"Starting frentes CSV fallback ingestion "
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
        message="Frentes CSV file loaded successfully.",
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
            f"Failed reading frentes CSV files "
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
        message="Failed reading frentes CSV files.",
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
        "frn_tx_payload_json",
        to_json(struct("*"))
    )
)

bronze_df = (
    source_with_payload_df
    .select(
        get_source_column(source_with_payload_df, "id")
            .cast(StringType())
            .alias("frn_id_frente"),

        get_source_column(source_with_payload_df, "titulo")
            .cast(StringType())
            .alias("frn_tx_titulo"),

        get_source_column(source_with_payload_df, "uri")
            .cast(StringType())
            .alias("frn_tx_uri"),

        get_source_column(source_with_payload_df, "dataCriacao")
            .cast(StringType())
            .alias("frn_dt_criacao"),

        get_source_column(source_with_payload_df, "idLegislatura")
            .cast(StringType())
            .alias("frn_id_legislatura"),

        get_source_column(source_with_payload_df, "telefone")
            .cast(StringType())
            .alias("frn_tx_telefone"),

        get_source_column(source_with_payload_df, "email")
            .cast(StringType())
            .alias("frn_tx_email"),

        get_source_column(source_with_payload_df, "keywords")
            .cast(StringType())
            .alias("frn_tx_keywords"),

        get_source_column(source_with_payload_df, "idSituacao")
            .cast(StringType())
            .alias("frn_id_situacao"),

        get_source_column(source_with_payload_df, "situacao")
            .cast(StringType())
            .alias("frn_tx_situacao"),

        get_source_column(source_with_payload_df, "urlWebsite")
            .cast(StringType())
            .alias("frn_tx_url_website"),

        get_source_column(source_with_payload_df, "urlDocumento")
            .cast(StringType())
            .alias("frn_tx_url_documento"),

        get_source_column(source_with_payload_df, "coordenador_id")
            .cast(StringType())
            .alias("frn_id_coordenador"),

        get_source_column(source_with_payload_df, "coordenador_uri")
            .cast(StringType())
            .alias("frn_tx_uri_coordenador"),

        get_source_column(source_with_payload_df, "coordenador_nome")
            .cast(StringType())
            .alias("frn_tx_nome_coordenador"),

        get_source_column(source_with_payload_df, "coordenador_siglaPartido")
            .cast(StringType())
            .alias("frn_tx_sigla_partido_coordenador"),

        get_source_column(source_with_payload_df, "coordenador_uriPartido")
            .cast(StringType())
            .alias("frn_tx_uri_partido_coordenador"),

        get_source_column(source_with_payload_df, "coordenador_siglaUf")
            .cast(StringType())
            .alias("frn_tx_sigla_uf_coordenador"),

        get_source_column(source_with_payload_df, "coordenador_idLegislatura")
            .cast(StringType())
            .alias("frn_id_legislatura_coordenador"),

        get_source_column(source_with_payload_df, "coordenador_urlFoto")
            .cast(StringType())
            .alias("frn_tx_url_foto_coordenador"),

        col("frn_tx_payload_json")
            .cast(StringType())
            .alias("frn_tx_payload_json"),

        col("aud_tx_arquivo_origem")
            .cast(StringType())
            .alias("aud_tx_arquivo_origem"),
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
        lit("csv_fallback/frentes.csv")
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
# MAGIC ## 4. Add Record Hash

# COMMAND ----------

bronze_df = add_hash(
    dataframe=bronze_df,
    columns=[
        "frn_id_frente",
        "frn_tx_payload_json",
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
        f"Bronze frentes CSV fallback table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw parliamentary front ingestion table from official Câmara dos Deputados CSV fallback files.

This Bronze table preserves parliamentary front records loaded from frentes.csv when the API endpoint /frentes is unavailable, unstable or slow.

Main characteristics:
- raw ingestion fidelity
- original payload preservation
- ingestion metadata
- record hash support
- auditability
- CSV fallback resilience
- compatibility with the original API Bronze schema
- additional CSV attributes preserved for downstream enrichment

Architecture note:
- The original API-compatible fields are preserved using the frn_* naming standard.
- Additional CSV attributes are also persisted as Bronze fields.
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source:
- csv_fallback/frentes.csv
"""

column_comments = {
    "frn_id_frente": "Parliamentary front identifier as provided by the official Câmara source.",
    "frn_tx_titulo": "Parliamentary front title as provided by the official Câmara source.",
    "frn_tx_uri": "Parliamentary front URI as provided by the official Câmara source.",

    "frn_dt_criacao": "Parliamentary front creation date as provided by the CSV source.",
    "frn_id_legislatura": "Legislature identifier associated with the parliamentary front.",
    "frn_tx_telefone": "Parliamentary front contact phone number.",
    "frn_tx_email": "Parliamentary front contact email.",
    "frn_tx_keywords": "Keywords associated with the parliamentary front.",
    "frn_id_situacao": "Parliamentary front status identifier.",
    "frn_tx_situacao": "Parliamentary front status description.",
    "frn_tx_url_website": "Parliamentary front official website URL.",
    "frn_tx_url_documento": "Parliamentary front official document URL.",

    "frn_id_coordenador": "Coordinator deputy identifier associated with the parliamentary front.",
    "frn_tx_uri_coordenador": "Coordinator deputy URI.",
    "frn_tx_nome_coordenador": "Coordinator deputy name.",
    "frn_tx_sigla_partido_coordenador": "Coordinator deputy political party acronym.",
    "frn_tx_uri_partido_coordenador": "Coordinator deputy political party URI.",
    "frn_tx_sigla_uf_coordenador": "Coordinator deputy federation unit acronym.",
    "frn_id_legislatura_coordenador": "Coordinator deputy legislature identifier.",
    "frn_tx_url_foto_coordenador": "Coordinator deputy photo URL.",

    "frn_tx_payload_json": "Original raw payload preserved from the CSV source as JSON.",
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
        f"Bronze frentes CSV fallback ingestion completed successfully "
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
        f"Bronze frentes CSV fallback ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE FRENTES CSV FALLBACK COMPLETED")
print("=" * 90)
print(f"Source Path: {SOURCE_FILE_PATH}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)